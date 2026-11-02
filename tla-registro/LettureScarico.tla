---------------------------- MODULE LettureScarico ----------------------------
(***************************************************************************)
(* Letture e scarico su S3 sopra il registro di ValidatingLog               *)
(* (docs/protocollo.md sezioni 6 e 7).                                      *)
(*                                                                          *)
(* Lettura: un client c (con il suo gettone = posizione piu' alta vista)    *)
(* legge la chiave k a posizione S da una replica r qualsiasi, anche        *)
(* isolata. Il nodo d'ingresso compone: scritture a k negli SST della sua   *)
(* vista del manifest (posizione M, anche vecchia) + scritture a k nei      *)
(* record (M, S] della replica. La replica deve servire solo se             *)
(* S <= confermataLocale (commit[r]) e M+1 >= inizio della sua finestra.    *)
(*                                                                          *)
(* Scarico: due worker leggono il manifest (posizione M, lista di oggetti,  *)
(* etag), prendono da una replica i record confermati (M, fino], scrivono   *)
(* un oggetto con nome unico, poi il manifest con PUT condizionale          *)
(* sull'etag. Le repliche possono scartare dal disco fino alla posizione    *)
(* del manifest (trimmed).                                                  *)
(*                                                                          *)
(* Controlli negativi:                                                      *)
(*  UnsafeServe      la replica serve se ha S in coda (ignora la confermata)*)
(*  IgnoreToken      il nodo d'ingresso ignora il gettone del client        *)
(*  UnconditionalPut il manifest si scrive senza condizione sull'etag       *)
(*  NameByStart      l'oggetto SST prende nome dalla sola posizione M       *)
(*  UnsafeFetch      il worker prende dalla replica anche record non        *)
(*                   confermati                                             *)
(*  DeleteOrphanBlind dopo un PUT condizionale fallito il worker cancella   *)
(*                   il proprio oggetto senza rileggere il manifest (il     *)
(*                   fallimento puo' essere il 412 del ritentativo di un    *)
(*                   PUT gia' riuscito con risposta persa)                  *)
(* Giro 2 (revisore di S1):                                                 *)
(*  ServeRamOnly     la replica serve solo dalla finestra in RAM            *)
(*                   (potatoFinoA = inizioFinestra - 1): le scritture tra   *)
(*                   60 s e lo scarico non le serve nessuno                 *)
(*  NoServeGuard     la replica serve anche se non ha piu' (M, S] sul disco *)
(*  LinNotReady      lettura lineare servita da un capo non ancora pronto   *)
(* Opzioni:                                                                 *)
(*  ReadPrepared     la S di ogni scrittura e' quella di una lettura        *)
(*                   composta servita davvero (non una S qualsiasi), e la   *)
(*                   chiave di sessione si legge con la stessa lettura      *)
(*                   (scatole + quaderno), non con l'oracolo                *)
(*  MaxLinReads      numero massimo di letture lineari                      *)
(* Disco: ogni replica ha diskStart (inizioDisco); TrimR la alza fino alla   *)
(* posizione del manifest + 1 e mai sopra la propria finestra. Reload e' la *)
(* ricarica da S3 di una replica rimasta sotto l'inizioDisco del capo.      *)
(***************************************************************************)
EXTENDS ValidatingLog

CONSTANTS Clients, Workers, Owner, MaxReads,
          UnsafeServe, IgnoreToken, UnconditionalPut, NameByStart, UnsafeFetch, DeleteOrphanBlind,
          ServeRamOnly, NoServeGuard, LinNotReady, ReadPrepared, MaxLinReads

VARIABLES
  token,     \* client -> posizione piu' alta vista
  reads,     \* numero di letture servite (limite del modello)
  readOk,    \* FALSE appena una lettura servita e' sbagliata (valore o monotonia); i dettagli
             \* della lettura stanno nei parametri dell'azione Read nella traccia di TLC
  wstate,    \* worker -> "idle" | "fetched" | "stored"
  wview,     \* worker -> vista del manifest letta: [pos, list, etag]
  wupto,     \* worker -> fine dell'intervallo preso
  wents,     \* worker -> record presi (M, fino]
  wname,     \* worker -> nome dell'oggetto scritto
  objects,   \* nome -> sequenza di record (contenuto dell'oggetto SST)
  deleted,   \* insieme dei nomi di oggetto cancellati
  manifest,  \* [pos, list, etag]
  manifests, \* insieme di tutte le viste del manifest mai pubblicate
  linReads   \* numero di letture lineari servite (limite del modello)

rvars == <<token, reads, readOk, wstate, wview, wupto, wents, wname, objects, deleted, manifests, manifest, linReads>>
allvars == <<vars, rvars>>

NoName == << >>
Min2(S) == CHOOSE x \in S : \A y \in S : x <= y

\* le posizioni delle scritture alla chiave k in una sequenza di record, con posizioni base+1..
WritesTo(seq, base, k, upto) ==
  LET P == {i \in 1..Len(seq) : base + i <= upto /\ \E w \in WritesOf(seq[i]) : w.k = k}
  IN IF P = {} THEN << >> ELSE
     LET n == Cardinality(P)
         f[j \in 1..n] == IF j = 1 THEN Min2(P) ELSE Min2({p \in P : p > f[j-1]})
     IN [j \in 1..n |-> base + f[j]]

\* contenuto di S3 secondo una vista del manifest: concatenazione degli oggetti elencati
RECURSIVE Concat(_)
Concat(list) == IF Len(list) = 0 THEN << >> ELSE Concat(SubSeq(list, 1, Len(list) - 1)) \o objects[list[Len(list)]]
S3Content(view) == Concat(view.list)

InitR ==
  /\ Init
  /\ token = [c \in Clients |-> 0]
  /\ reads = 0
  /\ readOk = TRUE
  /\ wstate = [w \in Workers |-> "idle"]
  /\ wview = [w \in Workers |-> [pos |-> 0, list |-> << >>, etag |-> 0]]
  /\ wupto = [w \in Workers |-> 0]
  /\ wents = [w \in Workers |-> << >>]
  /\ wname = [w \in Workers |-> NoName]
  /\ objects = [n \in {} |-> << >>]
  /\ deleted = {}
  /\ manifest = [pos |-> 0, list |-> << >>, etag |-> 0]
  /\ manifests = {[pos |-> 0, list |-> << >>, etag |-> 0]}
  /\ linReads = 0

(* ---------------------------------------------------------------- letture *)

MaxLen == Max({Len(log[r]) : r \in Replicas})
Max2(a, b) == IF a > b THEN a ELSE b

\* prima posizione che la replica r sa servire: dal disco (regola giusta: potatoFinoA = inizioDisco)
\* oppure solo dalla RAM (ServeRamOnly, la regola del primo giro letta alla lettera)
ServedFrom(r) == IF ServeRamOnly THEN wstart[r] ELSE diskStart[r]
\* la replica serve (M, S] solo se lo ha tutto (guardia), e comunque restituisce solo cio' che ha
CanServe(r, view, s) ==
  /\ alive[r]
  /\ IF UnsafeServe THEN s <= Len(log[r]) ELSE s <= commit[r]   \* regola sicura: S <= confermataLocale
  /\ view.pos <= s
  /\ NoServeGuard \/ view.pos + 1 >= ServedFrom(r)
FromPos(r, view) == Max2(view.pos + 1, ServedFrom(r))
\* la sequenza di record che il nodo d'ingresso compone: scatole della vista + quaderno in (M, S]
Composed(r, view, s) == S3Content(view) \o SubSeq(log[r], FromPos(r, view), s)

Read(c, r, k, view, s) ==
  /\ reads < MaxReads
  /\ IgnoreToken \/ s >= token[c]                       \* B: S non sotto il gettone del client
  /\ CanServe(r, view, s)
  /\ LET base == WritesTo(S3Content(view), 0, k, view.pos)
         win == WritesTo(SubSeq(log[r], FromPos(r, view), s), FromPos(r, view) - 1, k, s)
         exp == WritesTo(cmtLog, 0, k, s)
     IN readOk' = (readOk /\ base \o win = exp /\ s >= token[c])
  /\ token' = [token EXCEPT ![c] = Max({@, s})]
  /\ reads' = reads + 1
  /\ UNCHANGED <<wstate, wview, wupto, wents, wname, objects, deleted, manifests, manifest, linReads>>

\* lettura lineare (readConcern linearizable, ReadIndex): il capo risponde a POSIZIONE con la propria
\* confermata solo se e' ancora capo (epoca piu' alta: nel modello atomico equivale all'ACK di una
\* maggioranza nella propria epoca) E pronto (barriera del proprio mandato confermata). Senza
\* "pronto" (LinNotReady) un capo appena eletto risponde con una confermata piu' vecchia di una
\* scrittura gia' confermata al client dal capo precedente.
LinRead(c, l) ==
  /\ linReads < MaxLinReads /\ alive[l] /\ role[l] = "leader" /\ epoch[l] = MaxEpochNow
  /\ LinNotReady \/ LeaderReady(l)
  /\ readOk' = (readOk /\ \A t \in Txns : \A p \in outcome[t] : p <= commit[l])
  /\ token' = [token EXCEPT ![c] = Max({@, commit[l]})]
  /\ linReads' = linReads + 1
  /\ UNCHANGED <<reads, wstate, wview, wupto, wents, wname, objects, deleted, manifests, manifest>>

\* scrittura preparata su una lettura composta servita davvero (ReadPrepared): S = s della lettura,
\* e la chiave di sessione si legge nella stessa lettura (scatole della vista + quaderno in (M, S]).
\* La lettura deve essere esatta: la sequenza composta e' il prefisso confermato fino a S.
SubmitFromRead(l, t, r, view, s) ==
  /\ CanServe(r, view, s)
  /\ LET comp == Composed(r, view, s)
         seen == IF SessionLookup THEN {i \in 1..Len(comp) : comp[i].tx = t} ELSE {}
     IN /\ SubmitAt(l, t, s, seen)
        /\ readOk' = (readOk /\ comp = SubSeq(cmtLog, 1, s))
  /\ UNCHANGED <<token, reads, wstate, wview, wupto, wents, wname, objects, deleted, manifests, manifest, linReads>>

\* il client riceve la conferma di una propria scrittura: gettone = posizione confermata
\* (ogni transazione e' di un client: Owner, definito nel modulo MC)
LearnOutcome(c, t) ==
  /\ Owner[t] = c
  /\ \E p \in outcome[t] : p > token[c] /\ token' = [token EXCEPT ![c] = p]
  /\ UNCHANGED <<reads, readOk, wstate, wview, wupto, wents, wname, objects, deleted, manifests, manifest, linReads>>

(* ---------------------------------------------------------------- scarico *)

Fetch(w, r) ==
  /\ wstate[w] = "idle" /\ alive[r]
  /\ LET m == manifest.pos
         lim == IF UnsafeFetch THEN Len(log[r]) ELSE commit[r]
     IN \E upto \in (m + 1)..lim :
          /\ m + 1 >= diskStart[r]          \* LEGGI_INTERVALLO solo se M >= inizioDisco
          /\ wview' = [wview EXCEPT ![w] = manifest]
          /\ wupto' = [wupto EXCEPT ![w] = upto]
          /\ wents' = [wents EXCEPT ![w] = SubSeq(log[r], m + 1, upto)]
          /\ wstate' = [wstate EXCEPT ![w] = "fetched"]
  /\ UNCHANGED <<token, reads, readOk, wname, objects, deleted, manifests, manifest, linReads>>

StoreObject(w) ==
  /\ wstate[w] = "fetched"
  /\ LET name == IF NameByStart THEN <<"sst", wview[w].pos>> ELSE <<"sst", wview[w].pos, wupto[w], w>>
     IN /\ wname' = [wname EXCEPT ![w] = name]
        /\ objects' = [n \in DOMAIN objects \cup {name} |-> IF n = name THEN wents[w] ELSE objects[n]]
  /\ wstate' = [wstate EXCEPT ![w] = "stored"]
  /\ UNCHANGED <<token, reads, readOk, wview, wupto, wents, deleted, manifests, manifest, linReads>>

PutManifest(w) ==
  /\ wstate[w] = "stored"
  /\ IF UnconditionalPut \/ manifest.etag = wview[w].etag
     THEN LET nm == [pos |-> wupto[w], list |-> Append(wview[w].list, wname[w]), etag |-> manifest.etag + 1]
          IN /\ manifest' = nm
             /\ manifests' = manifests \cup {nm}
             \* la risposta arriva (idle) oppure si perde: il worker ritentera' il PUT e ricevera' un 412
             /\ \E lost \in BOOLEAN : wstate' = [wstate EXCEPT ![w] = IF lost THEN "stored" ELSE "idle"]
             /\ UNCHANGED <<objects, deleted>>
     ELSE \* PUT condizionale fallito (412)
          /\ UNCHANGED <<manifest, manifests, objects>>
          /\ IF DeleteOrphanBlind \/ ~(\E i \in 1..Len(manifest.list) : manifest.list[i] = wname[w])
             THEN deleted' = deleted \cup {wname[w]}   \* orfano (o creduto tale): cancellato
             ELSE UNCHANGED deleted                    \* rilettura del manifest: il proprio oggetto c'e', il PUT era riuscito
          /\ wstate' = [wstate EXCEPT ![w] = "idle"]
  /\ UNCHANGED <<token, reads, readOk, wview, wupto, wents, wname, linReads>>

\* la replica r scarta dal disco fino alla posizione del manifest (SCARICATO): diskStart sale al piu' a
\* manifest.pos + 1, e mai sopra l'inizio della propria finestra (la finestra in RAM e' una cache del
\* disco). Le voci sotto diskStart restano nella sequenza solo come (posizione, epoca).
TrimR(r) ==
  /\ alive[r]
  /\ \E d \in (diskStart[r] + 1)..Min(manifest.pos + 1, wstart[r]) :
       diskStart' = [diskStart EXCEPT ![r] = d]
  /\ UNCHANGED <<epoch, role, alive, log, commit, match, wstart, mapS, mapA, ids, attempts, outcome, cmtLog, losses>>
  /\ UNCHANGED rvars

\* ricarica da S3 (come InstallSnapshot di Raft): il seguace f e' sotto l'inizioDisco di un capo l, che
\* non puo' piu' mandargli le voci. f prende dal manifest M e le scatole; se la propria voce a M
\* coincide (stessa posizione ed epoca) tiene la propria coda, altrimenti butta tutto il log.
Reload(f) ==
  /\ alive[f] /\ role[f] = "follower"
  /\ \E l \in Replicas : alive[l] /\ role[l] = "leader" /\ epoch[l] >= epoch[f]
                         /\ MatchLen(log[l], log[f]) + 1 < diskStart[l]
  /\ LET M == manifest.pos
         content == S3Content(manifest)
         keep == Len(log[f]) >= M /\ (M = 0 \/ log[f][M] = content[M])
         newLog == IF keep THEN content \o SubSeq(log[f], M + 1, Len(log[f])) ELSE content
     IN /\ M + 1 > diskStart[f]
        /\ log' = [log EXCEPT ![f] = newLog]
        /\ diskStart' = [diskStart EXCEPT ![f] = M + 1]
        /\ wstart' = [wstart EXCEPT ![f] = Max2(@, M + 1)]
        /\ commit' = [commit EXCEPT ![f] = Max2(@, M)]
  /\ UNCHANGED <<epoch, role, alive, match, mapS, mapA, ids, attempts, outcome, cmtLog, losses>>
  /\ UNCHANGED rvars

\* le azioni del registro, con lo stato delle letture e dello scarico fermo
RElect(c, Q) == Elect(c, Q) /\ UNCHANGED rvars
RSubmit(l, t) == Submit(l, t) /\ UNCHANGED rvars
RReplicate(l, f, p, n) == Replicate(l, f, p, n) /\ UNCHANGED rvars
RAdvanceCommit(l) == AdvanceCommit(l) /\ UNCHANGED rvars
RAdvanceWindow(l) == AdvanceWindow(l) /\ UNCHANGED rvars
RLose(r) == Lose(r) /\ UNCHANGED rvars

NextR ==
  \/ \E c \in Replicas, Q \in SUBSET Replicas : RElect(c, Q)
  \/ ~ReadPrepared /\ \E l \in Replicas, t \in Txns : RSubmit(l, t)
  \/ ReadPrepared /\ \E l, r \in Replicas, t \in Txns, view \in manifests, s \in 0..MaxCommit :
        SubmitFromRead(l, t, r, view, s)
  \/ \E l, f \in Replicas, p \in 0..MaxLog, n \in 0..1 : RReplicate(l, f, p, n)
  \/ \E l \in Replicas : RAdvanceCommit(l) \/ RAdvanceWindow(l) \/ RLose(l) \/ TrimR(l) \/ Reload(l)
  \/ /\ UNCHANGED vars
     /\ \/ \E c \in Clients, r \in Replicas, k \in Keys, view \in manifests, s \in 0..MaxLen : Read(c, r, k, view, s)
        \/ \E c \in Clients, t \in Txns : LearnOutcome(c, t)
        \/ \E c \in Clients, l \in Replicas : LinRead(c, l)
        \/ \E w \in Workers, r \in Replicas : Fetch(w, r)
        \/ \E w \in Workers : StoreObject(w) \/ PutManifest(w)

SpecR == InitR /\ [][NextR]_allvars

(* ------------------------------------------------------------- invarianti *)

\* (a) una lettura a S restituisce esattamente le scritture confermate alla chiave con posizione <= S,
\* e (b) nessuna lettura servita e' piu' vecchia della posizione gia' vista dal client
ReadsCorrect == readOk
MonotonicReads == readOk

\* (c) lo scarico non perde e non duplica: il contenuto di S3 secondo il manifest corrente e'
\*     esattamente il prefisso confermato fino alla posizione del manifest
UploadExact ==
  /\ manifest.pos <= Len(cmtLog)
  /\ S3Content(manifest) = SubSeq(cmtLog, 1, manifest.pos)
  /\ \A i \in 1..Len(manifest.list) : manifest.list[i] \notin deleted

\* (d) niente scartato dal disco che non sia in S3
NothingLost == \A r \in Replicas : diskStart[r] <= manifest.pos + 1

\* (d2) ogni record confermato e' in S3 oppure sul disco (sopra inizioDisco) di una replica viva
DurableOrS3 ==
  \A i \in 1..Len(cmtLog) :
     \/ i <= manifest.pos
     \/ \E r \in Alive : diskStart[r] <= i /\ i <= Len(log[r]) /\ log[r][i] = cmtLog[i]

\* (f) le letture si possono servire: almeno una replica viva serve dalla posizione del manifest
ReadServable == \E r \in Alive : manifest.pos + 1 >= ServedFrom(r)

\* (g) il gettone non va mai oltre la storia confermata
TokenConfirmed == \A c \in Clients : token[c] <= Len(cmtLog)

\* (e) il manifest non torna indietro
ManifestMonotonic == [][manifest.pos' >= manifest.pos]_allvars

=============================================================================
