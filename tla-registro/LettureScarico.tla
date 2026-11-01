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
(***************************************************************************)
EXTENDS ValidatingLog

CONSTANTS Clients, Workers, Owner, MaxReads,
          UnsafeServe, IgnoreToken, UnconditionalPut, NameByStart, UnsafeFetch

VARIABLES
  token,     \* client -> posizione piu' alta vista
  reads,     \* numero di letture servite (limite del modello)
  lastRead,  \* ultima lettura servita: [c, k, s, tok, got, exp]
  wstate,    \* worker -> "idle" | "fetched" | "stored"
  wview,     \* worker -> vista del manifest letta: [pos, list, etag]
  wupto,     \* worker -> fine dell'intervallo preso
  wents,     \* worker -> record presi (M, fino]
  wname,     \* worker -> nome dell'oggetto scritto
  objects,   \* nome -> sequenza di record (contenuto dell'oggetto SST)
  manifest,  \* [pos, list, etag]
  manifests, \* insieme di tutte le viste del manifest mai pubblicate
  trimmed    \* posizione fino alla quale le repliche possono aver scartato dal disco

rvars == <<token, reads, lastRead, wstate, wview, wupto, wents, wname, objects, manifests, manifest, trimmed>>
allvars == <<vars, rvars>>

NoRead == [c |-> "none", k |-> "none", s |-> 0, tok |-> 0, got |-> << >>, exp |-> << >>]
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
  /\ lastRead = NoRead
  /\ wstate = [w \in Workers |-> "idle"]
  /\ wview = [w \in Workers |-> [pos |-> 0, list |-> << >>, etag |-> 0]]
  /\ wupto = [w \in Workers |-> 0]
  /\ wents = [w \in Workers |-> << >>]
  /\ wname = [w \in Workers |-> NoName]
  /\ objects = [n \in {} |-> << >>]
  /\ manifest = [pos |-> 0, list |-> << >>, etag |-> 0]
  /\ manifests = {[pos |-> 0, list |-> << >>, etag |-> 0]}
  /\ trimmed = 0

(* ---------------------------------------------------------------- letture *)

MaxLen == Max({Len(log[r]) : r \in Replicas})

Read(c, r, k, view, s) ==
  /\ reads < MaxReads /\ alive[r]
  /\ IgnoreToken \/ s >= token[c]                       \* B: S non sotto il gettone del client
  /\ IF UnsafeServe THEN s <= Len(log[r]) ELSE s <= commit[r]   \* la replica ha S (regola sicura: confermata)
  /\ view.pos <= s
  /\ view.pos + 1 >= wstart[r]                          \* la finestra della replica copre (M, S]
  /\ LET base == WritesTo(S3Content(view), 0, k, view.pos)
         win == WritesTo(SubSeq(log[r], view.pos + 1, s), view.pos, k, s)
         exp == WritesTo(cmtLog, 0, k, s)
     IN lastRead' = [c |-> c, k |-> k, s |-> s, tok |-> token[c], got |-> base \o win, exp |-> exp]
  /\ token' = [token EXCEPT ![c] = Max({@, s})]
  /\ reads' = reads + 1
  /\ UNCHANGED <<wstate, wview, wupto, wents, wname, objects, manifests, manifest, trimmed>>

\* il client riceve la conferma di una propria scrittura: gettone = posizione confermata
\* (ogni transazione e' di un client: Owner, definito nel modulo MC)
LearnOutcome(c, t) ==
  /\ Owner[t] = c
  /\ \E p \in outcome[t] : p > token[c] /\ token' = [token EXCEPT ![c] = p]
  /\ UNCHANGED <<reads, lastRead, wstate, wview, wupto, wents, wname, objects, manifests, manifest, trimmed>>

(* ---------------------------------------------------------------- scarico *)

Fetch(w, r) ==
  /\ wstate[w] = "idle" /\ alive[r]
  /\ LET m == manifest.pos
         lim == IF UnsafeFetch THEN Len(log[r]) ELSE commit[r]
     IN \E upto \in (m + 1)..lim :
          /\ wview' = [wview EXCEPT ![w] = manifest]
          /\ wupto' = [wupto EXCEPT ![w] = upto]
          /\ wents' = [wents EXCEPT ![w] = SubSeq(log[r], m + 1, upto)]
          /\ wstate' = [wstate EXCEPT ![w] = "fetched"]
  /\ UNCHANGED <<token, reads, lastRead, wname, objects, manifests, manifest, trimmed>>

StoreObject(w) ==
  /\ wstate[w] = "fetched"
  /\ LET name == IF NameByStart THEN <<"sst", wview[w].pos>> ELSE <<"sst", wview[w].pos, wupto[w], w>>
     IN /\ wname' = [wname EXCEPT ![w] = name]
        /\ objects' = [n \in DOMAIN objects \cup {name} |-> IF n = name THEN wents[w] ELSE objects[n]]
  /\ wstate' = [wstate EXCEPT ![w] = "stored"]
  /\ UNCHANGED <<token, reads, lastRead, wview, wupto, wents, manifests, manifest, trimmed>>

PutManifest(w) ==
  /\ wstate[w] = "stored"
  /\ IF UnconditionalPut \/ manifest.etag = wview[w].etag
     THEN LET nm == [pos |-> wupto[w], list |-> Append(wview[w].list, wname[w]), etag |-> manifest.etag + 1]
          IN /\ manifest' = nm
             /\ manifests' = manifests \cup {nm}
     ELSE UNCHANGED <<manifest, manifests>>   \* PUT condizionale fallito: l'oggetto resta orfano
  /\ wstate' = [wstate EXCEPT ![w] = "idle"]
  /\ UNCHANGED <<token, reads, lastRead, wview, wupto, wents, wname, objects, trimmed>>

\* le repliche scartano dal disco fino alla posizione del manifest
Trim ==
  /\ trimmed < manifest.pos
  /\ trimmed' = manifest.pos
  /\ UNCHANGED <<token, reads, lastRead, wstate, wview, wupto, wents, wname, objects, manifests, manifest>>

\* le azioni del registro, con lo stato delle letture e dello scarico fermo
RElect(c, Q) == Elect(c, Q) /\ UNCHANGED rvars
RSubmit(l, t) == Submit(l, t) /\ UNCHANGED rvars
RReplicate(l, f) == Replicate(l, f) /\ UNCHANGED rvars
RHeartbeat(l, f) == Heartbeat(l, f) /\ UNCHANGED rvars
RAdvanceCommit(l) == AdvanceCommit(l) /\ UNCHANGED rvars
RAdvanceWindow(l) == AdvanceWindow(l) /\ UNCHANGED rvars
RLose(r) == Lose(r) /\ UNCHANGED rvars

NextR ==
  \/ \E c \in Replicas, Q \in SUBSET Replicas : RElect(c, Q)
  \/ \E l \in Replicas, t \in Txns : RSubmit(l, t)
  \/ \E l, f \in Replicas : RReplicate(l, f) \/ RHeartbeat(l, f)
  \/ \E l \in Replicas : RAdvanceCommit(l) \/ RAdvanceWindow(l) \/ RLose(l)
  \/ /\ UNCHANGED vars
     /\ \/ \E c \in Clients, r \in Replicas, k \in Keys, view \in manifests, s \in 0..MaxLen : Read(c, r, k, view, s)
        \/ \E c \in Clients, t \in Txns : LearnOutcome(c, t)
        \/ \E w \in Workers, r \in Replicas : Fetch(w, r)
        \/ \E w \in Workers : StoreObject(w) \/ PutManifest(w)
        \/ Trim

SpecR == InitR /\ [][NextR]_allvars

(* ------------------------------------------------------------- invarianti *)

\* (a) una lettura a S restituisce esattamente le scritture confermate alla chiave con posizione <= S
ReadsCorrect == lastRead.got = lastRead.exp

\* (b) nessuna lettura servita e' piu' vecchia della posizione gia' vista dal client
MonotonicReads == lastRead.s >= lastRead.tok

\* (c) lo scarico non perde e non duplica: il contenuto di S3 secondo il manifest corrente e'
\*     esattamente il prefisso confermato fino alla posizione del manifest
UploadExact ==
  /\ manifest.pos <= Len(cmtLog)
  /\ S3Content(manifest) = SubSeq(cmtLog, 1, manifest.pos)

\* (d) niente scartato dal disco che non sia in S3
NothingLost == trimmed <= manifest.pos

\* (e) il manifest non torna indietro
ManifestMonotonic == [][manifest.pos' >= manifest.pos]_allvars

=============================================================================
