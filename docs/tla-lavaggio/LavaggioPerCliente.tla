--------------------------- MODULE LavaggioPerCliente ---------------------------
(***************************************************************************)
(* Il lavaggio per cliente (compaction raggruppata per chiave cliente) e   *)
(* lo scambio delle scatole (SST) nel foglio indice (manifest), con:       *)
(*  - scrittori che cambiano un documento e, nello stesso record del       *)
(*    quaderno (log), le sue voci di indice;                                *)
(*  - la cancellazione di un cliente (GDPR) come record del quaderno e      *)
(*    lista dei cancellati (ledger) nel foglio, poi riscrittura mirata;     *)
(*  - lo scarico (flush) che sigilla la coda del quaderno in una scatola;   *)
(*  - il lavaggio che legge alcune scatole di un cliente sopra il tetto di  *)
(*    pile, scrive una scatola per cliente e scambia il foglio con una      *)
(*    sola scrittura condizionale (riesce solo se le scatole di partenza    *)
(*    sono ancora nel foglio);                                              *)
(*  - lettori con istantanea (foglio + posizione S) che leggono indice e    *)
(*    documento in due momenti;                                             *)
(*  - un ramo (fork) con la spilla entro un tetto di tempo FHold;           *)
(*  - la pulizia (GC) con eta' minima Grace;                                *)
(*  - crash a meta' di scarico, lavaggio e riscrittura mirata.              *)
(* Vedi PROPRIETA.md e RISULTATI.md in questa cartella.                    *)
(*                                                                          *)
(* Il tempo: ogni giro di pulizia (Gc) fa invecchiare di 1 le scatole non   *)
(* usate, i lettori e gli scrittori in volo. Le eta' si fermano al tetto    *)
(* (sature), cosi' il modello e' finito e si puo' verificare la vivezza.   *)
(*                                                                          *)
(* Controlli negativi (costanti booleane, FALSE nel modello giusto):        *)
(*  NoInputCheck   lo scambio del lavaggio non controlla le scatole di      *)
(*                 partenza                                                 *)
(*  BlindPut       il lavaggio scrive il foglio intero letto all'inizio     *)
(*  DropTombAlways il lavaggio toglie le lapidi anche con dati sotto        *)
(*  SplitSwap      il lavaggio scambia in due scritture: documenti, indici  *)
(*  ReaderTwoSnap  il lettore rilegge il foglio prima del documento         *)
(*  ForkNoLedger   il ramo non eredita la lista dei cancellati              *)
(*  GcNoPins       la pulizia non guarda la spilla del ramo                 *)
(*  GcOnlyRemoved  la pulizia cancella solo scatole uscite da un foglio     *)
(*  Grace = 0      nessuna eta' minima                                      *)
(*  MinSources = 3 il lavaggio parte solo con almeno 3 scatole              *)
(***************************************************************************)
EXTENDS Naturals, FiniteSets, TLC

CONSTANTS Clients, Colls, Vals,
          MaxSeq, MaxBoxes, Workers, Readers,
          Cap, MinSources, Grace, Hold, RHold, FHold, MaxForks, MaxCrash, MaxErase,
          NoInputCheck, BlindPut, DropTombAlways, SplitSwap, ReaderTwoSnap,
          ForkNoLedger, GcNoPins, GcOnlyRemoved,
          Track

ASSUME Hold < Grace \/ Grace = 0
ASSUME RHold < Grace \/ Grace = 0
ASSUME FHold < Grace \/ Grace = 0
\* il quaderno tronca con un giro di ritardo: basta se un lettore dura al massimo un giro
ASSUME RHold <= 1

Keys  == Clients \X Colls
\* i clienti sono intercambiabili: TLC puo' contare una volta sola gli stati uguali a meno di
\* uno scambio di nomi (solo nei modelli di sicurezza)
SymClients == Permutations(Clients)
Boxes == 1..MaxBoxes
Seqs  == 1..MaxSeq
Min(a, b) == IF a < b THEN a ELSE b
NoMf  == [boxes |-> {}, flushed |-> 0, ledger |-> {}]

\* una voce in una scatola o nel quaderno:
\*  t = "d" documento (v = valore, 0 = cancellato; on = v # 0)
\*  t = "i" voce di indice per il valore v (on = accesa, FALSE = lapide)
Rec(t, c, k, v, on, s) == [t |-> t, c |-> c, k |-> k, v |-> v, on |-> on, s |-> s]

VARIABLES
  seqNow,   \* ultima posizione confermata nel quaderno
  hist,     \* posizione -> record del quaderno [t |-> "w"|"x"|"none", c, k, v]
  logE,     \* posizione -> voci (documento + indice) del record
  tr,       \* fantasma: posizione -> verita' (chiave -> valore) dopo quel record
  trim,     \* il quaderno ha gia' buttato le posizioni 1..trim (troncamento dopo lo scarico)
  pf,       \* posizione scaricata vista al giro di pulizia precedente (ritardo di un giro)
  boxSt,    \* scatola -> "none" | "live" | "gone"
  content,  \* scatola -> voci (immutabili finche' il nome non si riusa)
  bage,     \* scatola -> giri da quando e' nata o uscita dall'ultimo foglio o spilla
  wasIn,    \* scatola -> e' mai stata in un foglio (serve solo a GcOnlyRemoved)
  mf,       \* il foglio: [boxes, flushed, ledger]; ledger = insieme di <<cliente, E>>
  fst, fbox, fFrom, fTo, fX, fage,                \* lo scarico
  cst, cin, cres, cout, cage, cM,              \* i lavaggi (per worker)
  xst, xin, xres, xout, xage,                  \* la riscrittura mirata
  rst, rB, rF, rS, rG, rc, rk, ridx, rage,     \* i lettori
  kst, kB, kF, kS, kG, kL, kX, kage, nForks,   \* il ramo
  crashes,  \* crash avvenuti (limite del modello)
  bad,      \* errori visti dai lettori: "idx" | "doc" | "ver" | "fail"
  seen      \* fantasma, solo con Track: casi rari gia' accaduti

logv  == <<seqNow, hist, logE, tr, trim, pf>>
boxv  == <<boxSt, content, bage, wasIn>>
fv    == <<fst, fbox, fFrom, fTo, fX, fage>>
cv    == <<cst, cin, cres, cout, cage, cM>>
xv    == <<xst, xin, xres, xout, xage>>
rv    == <<rst, rB, rF, rS, rG, rc, rk, ridx, rage>>
kv    == <<kst, kB, kF, kS, kG, kL, kX, kage, nForks>>
vars  == <<logv, boxv, mf, fv, cv, xv, rv, kv, crashes, bad, seen>>

Mark(ev) == IF Track THEN seen' = seen \cup {ev} ELSE UNCHANGED seen

-----------------------------------------------------------------------------
(* Come si legge una vista: scatole B, quaderno da F+1 a S, lista G.        *)
(* Le posizioni gia' troncate (<= trim) non si possono piu' leggere.       *)

Max(a, b) == IF a > b THEN a ELSE b
LogEnt(F, S) == UNION {logE[s] : s \in (Max(F, trim)+1)..S}
LogX(F, S)   == {<<hist[s].c, s>> : s \in {q \in (Max(F, trim)+1)..S : hist[q].t = "x"}}
Covered(e, X) == \E x \in X : x[1] = e.c /\ e.s < x[2]
SameKey(e, f) == e.t = f.t /\ e.c = f.c /\ e.k = f.k /\ (e.t = "i" => e.v = f.v)
Latest(es) == {e \in es : \A f \in es : SameKey(e, f) => f.s <= e.s}

\* vista esplicita: scatole B, voci del quaderno L, cancellazioni X
VisV(B, L, X) == {e \in UNION {content[b] : b \in B} \cup L : ~Covered(e, X)}

DocValV(B, L, X, c, k) ==
  LET es == {e \in VisV(B, L, X) : e.t = "d" /\ e.c = c /\ e.k = k}
  IN IF es = {} THEN 0 ELSE (CHOOSE e \in Latest(es) : TRUE).v

IdxSetV(B, L, X, c, k) ==
  LET es == {e \in VisV(B, L, X) : e.t = "i" /\ e.c = c /\ e.k = k}
  IN {e.v : e \in {f \in Latest(es) : f.on}}

DocVal(B, F, S, G, c, k) == DocValV(B, LogEnt(F, S), G \cup LogX(F, S), c, k)
IdxSet(B, F, S, G, c, k) == IdxSetV(B, LogEnt(F, S), G \cup LogX(F, S), c, k)

Exp(v) == IF v = 0 THEN {} ELSE {v}

\* scatole del foglio dove sta un cliente: le sue pile
BoxesOf(c, B) == {b \in B : \E e \in content[b] : e.c = c}
Piles(c) == Cardinality(BoxesOf(c, mf.boxes))
Rel(B, c, k) == {b \in B : \E e \in content[b] : e.c = c /\ e.k = k}

Live == {b \in Boxes : boxSt[b] = "live"}
PinB == IF kst = "live" THEN kB ELSE {}

\* nomi di scatola che qualcuno ricorda: non si riusano
Ref == mf.boxes \cup PinB \cup kB \cup UNION {rB[r] : r \in Readers}
       \cup UNION {cin[w] \cup cout[w] : w \in Workers} \cup xin \cup xout
       \cup (IF fbox # 0 THEN {fbox} ELSE {})
Free == {b \in Boxes : boxSt[b] # "live" /\ b \notin Ref}
NewBox == CHOOSE b \in Free : \A b2 \in Free : b <= b2

\* scrive la scatola b con le voci es (nuovo nome)
Alloc(b, es) ==
  /\ boxSt' = [boxSt EXCEPT ![b] = "live"]
  /\ content' = [content EXCEPT ![b] = es]
  /\ bage' = [bage EXCEPT ![b] = 0]
  /\ wasIn' = [wasIn EXCEPT ![b] = FALSE]

\* le scatole in S escono da un foglio o da una spilla: l'eta' riparte
Touch(S) == bage' = [b \in Boxes |-> IF b \in S THEN 0 ELSE bage[b]]
\* wasIn serve solo al negativo GcOnlyRemoved e ai controlli di raggiungibilita'
EnterMf(S) == IF GcOnlyRemoved \/ Track
                THEN wasIn' = [b \in Boxes |-> IF b \in S THEN TRUE ELSE wasIn[b]]
                ELSE UNCHANGED wasIn

-----------------------------------------------------------------------------
Init ==
  /\ seqNow = 0
  /\ hist = [s \in Seqs |-> [t |-> "none", c |-> "-", k |-> "-", v |-> 0]]
  /\ logE = [s \in Seqs |-> {}]
  /\ tr = [s \in 0..MaxSeq |-> [x \in Keys |-> 0]]
  /\ trim = 0 /\ pf = 0
  /\ boxSt = [b \in Boxes |-> "none"]
  /\ content = [b \in Boxes |-> {}]
  /\ bage = [b \in Boxes |-> 0]
  /\ wasIn = [b \in Boxes |-> FALSE]
  /\ mf = NoMf
  /\ fst = "idle" /\ fbox = 0 /\ fFrom = 0 /\ fTo = 0 /\ fX = {} /\ fage = 0
  /\ cst = [w \in Workers |-> "idle"]
  /\ cin = [w \in Workers |-> {}]
  /\ cres = [w \in Workers |-> {}]
  /\ cout = [w \in Workers |-> {}]
  /\ cage = [w \in Workers |-> 0]
  /\ cM = [w \in Workers |-> NoMf]
  /\ xst = "idle" /\ xin = {} /\ xres = {} /\ xout = {} /\ xage = 0
  /\ rst = [r \in Readers |-> "idle"]
  /\ rB = [r \in Readers |-> {}]
  /\ rF = [r \in Readers |-> 0]
  /\ rS = [r \in Readers |-> 0]
  /\ rG = [r \in Readers |-> {}]
  /\ rc = [r \in Readers |-> "-"]
  /\ rk = [r \in Readers |-> "-"]
  /\ ridx = [r \in Readers |-> {}]
  /\ rage = [r \in Readers |-> 0]
  /\ kst = "idle" /\ kB = {} /\ kF = 0 /\ kS = 0 /\ kG = {} /\ kL = {} /\ kX = {}
  /\ kage = 0 /\ nForks = 0
  /\ crashes = 0
  /\ bad = {}
  /\ seen = {}

-----------------------------------------------------------------------------
(* Scrittori: un record del quaderno per scrittura (documento + indice).    *)

Write(c, k, v) ==
  /\ seqNow < MaxSeq
  /\ v # tr[seqNow][<<c, k>>]
  /\ LET s   == seqNow + 1
         old == tr[seqNow][<<c, k>>]
     IN /\ seqNow' = s
        /\ hist' = [hist EXCEPT ![s] = [t |-> "w", c |-> c, k |-> k, v |-> v]]
        /\ logE' = [logE EXCEPT ![s] =
                      {Rec("d", c, k, v, v # 0, s)}
                      \cup (IF old # 0 THEN {Rec("i", c, k, old, FALSE, s)} ELSE {})
                      \cup (IF v # 0 THEN {Rec("i", c, k, v, TRUE, s)} ELSE {})]
        /\ tr' = [tr EXCEPT ![s] = [tr[seqNow] EXCEPT ![<<c, k>>] = v]]
        /\ UNCHANGED <<trim, pf>>
        /\ IF Track /\ \E q \in 1..seqNow : hist[q].t = "x" /\ hist[q].c = c
             THEN seen' = seen \cup {"ricreatoDopoCancellazione"} ELSE UNCHANGED seen
  /\ UNCHANGED <<boxv, mf, fv, cv, xv, rv, kv, crashes, bad>>

\* cancellazione di un cliente: un record del quaderno alla posizione E
Erase(c) ==
  /\ seqNow < MaxSeq
  /\ Cardinality({q \in Seqs : hist[q].t = "x"}) < MaxErase
  /\ \E k \in Colls : tr[seqNow][<<c, k>>] # 0
  /\ LET s == seqNow + 1
     IN /\ seqNow' = s
        /\ hist' = [hist EXCEPT ![s] = [t |-> "x", c |-> c, k |-> "-", v |-> 0]]
        /\ UNCHANGED <<logE, trim, pf>>
        /\ tr' = [tr EXCEPT ![s] = [x \in Keys |-> IF x[1] = c THEN 0 ELSE tr[seqNow][x]]]
  /\ UNCHANGED <<boxv, mf, fv, cv, xv, rv, kv, crashes, bad, seen>>

-----------------------------------------------------------------------------
(* Lo scarico (flush): la coda del quaderno in una scatola mescolata.       *)

FlushWrite ==
  /\ fst = "idle" /\ mf.flushed < seqNow
  /\ LET F  == mf.flushed
         es == {e \in LogEnt(F, seqNow) : ~Covered(e, LogX(F, seqNow))}
     IN /\ fFrom' = F /\ fTo' = seqNow /\ fX' = LogX(F, seqNow) /\ fage' = 0 /\ fst' = "wrote"
        /\ IF es = {}
             THEN /\ fbox' = 0 /\ UNCHANGED boxv
             ELSE /\ Free # {} /\ fbox' = NewBox /\ Alloc(NewBox, es)
  /\ UNCHANGED <<logv, mf, cv, xv, rv, kv, crashes, bad, seen>>

\* scrittura condizionale del foglio (If-Match, con riprova che riapplica la modifica)
FlushCommit ==
  /\ fst = "wrote" /\ fage <= Hold /\ mf.flushed = fFrom
  /\ LET nb == IF fbox # 0 THEN {fbox} ELSE {}
     IN /\ mf' = [boxes |-> mf.boxes \cup nb, flushed |-> fTo,
                  ledger |-> mf.ledger \cup fX]
        /\ EnterMf(nb)
  /\ fst' = "idle" /\ fbox' = 0 /\ fFrom' = 0 /\ fTo' = 0 /\ fX' = {} /\ fage' = 0
  /\ UNCHANGED <<logv, boxSt, content, bage, cv, xv, rv, kv, crashes, bad, seen>>

FlushStop(crash) ==
  /\ fst # "idle"
  /\ IF crash THEN crashes < MaxCrash /\ crashes' = crashes + 1
              ELSE fage > Hold /\ UNCHANGED crashes
  /\ fst' = "idle" /\ fbox' = 0 /\ fFrom' = 0 /\ fTo' = 0 /\ fX' = {} /\ fage' = 0
  /\ IF crash /\ fbox # 0 THEN Mark("crashConOrfano") ELSE UNCHANGED seen
  /\ UNCHANGED <<logv, boxv, mf, cv, xv, rv, kv, bad>>

-----------------------------------------------------------------------------
(* Il lavaggio per cliente.                                                 *)

IsTomb(e) == ~e.on
\* nel foglio M, fuori dalle scatole In, c'e' una versione piu' vecchia della stessa chiave
OlderOutside(e, B) == \E b \in B : \E f \in content[b] : SameKey(e, f) /\ f.s < e.s

Merged(In, M) ==
  LET all  == {e \in UNION {content[b] : b \in In} : ~Covered(e, M.ledger)}
      last == Latest(all)
  IN {e \in last : ~(IsTomb(e) /\ (DropTombAlways \/ ~OlderOutside(e, M.boxes \ In)))}

GroupKey(e) == IF SplitSwap THEN <<e.c, e.t>> ELSE <<e.c, "*">>
Groups(S) == {{e \in S : GroupKey(e) = g} : g \in {GroupKey(e) : e \in S}}

CIdle(w) ==
  /\ cst' = [cst EXCEPT ![w] = "idle"]
  /\ cin' = [cin EXCEPT ![w] = {}]
  /\ cres' = [cres EXCEPT ![w] = {}]
  /\ cout' = [cout EXCEPT ![w] = {}]
  /\ cage' = [cage EXCEPT ![w] = 0]
  /\ cM' = [cM EXCEPT ![w] = NoMf]

\* sceglie un cliente sopra il tetto e un gruppo qualsiasi di sue scatole (strategia libera)
CompactRead(w) ==
  /\ cst[w] = "idle"
  /\ \E c \in Clients :
       /\ Piles(c) > Cap
       /\ \E In \in SUBSET BoxesOf(c, mf.boxes) :
            /\ Cardinality(In) >= MinSources
            /\ cst' = [cst EXCEPT ![w] = "read"]
            /\ cin' = [cin EXCEPT ![w] = In]
            /\ cres' = [cres EXCEPT ![w] = Groups(Merged(In, mf))]
            /\ cout' = [cout EXCEPT ![w] = {}]
            /\ cage' = [cage EXCEPT ![w] = 0]
            /\ cM' = [cM EXCEPT ![w] = IF BlindPut THEN mf ELSE NoMf]
            /\ IF Track /\ Merged(In, mf) # Latest(UNION {content[b] : b \in In})
                 THEN seen' = seen \cup {"lapideTolta"} ELSE UNCHANGED seen
  /\ UNCHANGED <<logv, boxv, mf, fv, xv, rv, kv, crashes, bad>>

\* scrive una scatola (un cliente) alla volta
CompactWrite(w) ==
  /\ cst[w] = "read" /\ cres[w] # {} /\ cage[w] <= Hold /\ Free # {}
  /\ \E grp \in cres[w] :
        /\ Alloc(NewBox, grp)
        /\ cres' = [cres EXCEPT ![w] = @ \ {grp}]
        /\ cout' = [cout EXCEPT ![w] = @ \cup {NewBox}]
  /\ UNCHANGED <<logv, mf, fv, cst, cin, cage, cM, xv, rv, kv, crashes, bad, seen>>

DocBoxes(S) == {b \in S : \E e \in content[b] : e.t = "d"}

CompactCommit(w) ==
  /\ cst[w] = "read" /\ cres[w] = {} /\ cage[w] <= Hold
  /\ BlindPut \/ NoInputCheck \/ cin[w] \subseteq mf.boxes
  /\ LET first == IF SplitSwap THEN DocBoxes(cout[w]) ELSE cout[w]
         base  == IF BlindPut THEN cM[w] ELSE mf
     IN /\ mf' = [base EXCEPT !.boxes = (@ \ cin[w]) \cup first]
        /\ Touch(cin[w] \cap mf.boxes)
        /\ EnterMf(first)
        /\ IF SplitSwap
             THEN /\ cst' = [cst EXCEPT ![w] = "half"]
                  /\ cout' = [cout EXCEPT ![w] = @ \ first]
                  /\ cin' = [cin EXCEPT ![w] = {}]
                  /\ UNCHANGED <<cres, cage, cM>>
             ELSE CIdle(w)
  /\ IF Track /\ \E r \in Readers : rst[r] # "idle" THEN seen' = seen \cup {"scambioConLettore"}
     ELSE UNCHANGED seen
  /\ UNCHANGED <<logv, boxSt, content, fv, xv, rv, kv, crashes, bad>>

\* solo con SplitSwap: la seconda scrittura del foglio, con le scatole degli indici
CompactCommit2(w) ==
  /\ cst[w] = "half"
  /\ mf' = [mf EXCEPT !.boxes = @ \cup cout[w]]
  /\ EnterMf(cout[w])
  /\ CIdle(w)
  /\ UNCHANGED <<logv, boxSt, content, bage, fv, xv, rv, kv, crashes, bad, seen>>

\* lo scambio fallisce: una scatola di partenza non c'e' piu' (le scatole scritte restano orfane)
CompactFail(w) ==
  /\ cst[w] = "read" /\ cres[w] = {} /\ cage[w] <= Hold
  /\ ~BlindPut /\ ~NoInputCheck /\ ~(cin[w] \subseteq mf.boxes)
  /\ CIdle(w)
  /\ IF mf.ledger # {} \/ xst # "idle" THEN Mark("lavaggioFallitoPerCancellazione") ELSE UNCHANGED seen
  /\ UNCHANGED <<logv, boxv, mf, fv, xv, rv, kv, crashes, bad>>

CompactStop(w, crash) ==
  /\ cst[w] \in {"read", "half"}
  /\ IF crash THEN crashes < MaxCrash /\ crashes' = crashes + 1
              ELSE cst[w] = "read" /\ cage[w] > Hold /\ UNCHANGED crashes
  /\ CIdle(w)
  /\ IF crash /\ cout[w] # {} THEN Mark("crashConOrfano") ELSE UNCHANGED seen
  /\ UNCHANGED <<logv, boxv, mf, fv, xv, rv, kv, bad>>

-----------------------------------------------------------------------------
(* La cancellazione mirata: riscrive solo le scatole con dati vecchi del    *)
(* cliente cancellato, poi toglie la riga dalla lista.                      *)

XIdle ==
  /\ xst' = "idle" /\ xin' = {} /\ xres' = {} /\ xout' = {} /\ xage' = 0

EraseRead ==
  /\ xst = "idle"
  /\ \E x \in mf.ledger :
       LET In == {b \in mf.boxes : \E e \in content[b] : e.c = x[1] /\ e.s < x[2]}
       IN /\ In # {}
          /\ xin' = In
          /\ xres' = {e \in UNION {content[b] : b \in In} : ~(e.c = x[1] /\ e.s < x[2])}
          /\ xout' = {} /\ xage' = 0 /\ xst' = "read"
  /\ UNCHANGED <<logv, boxv, mf, fv, cv, rv, kv, crashes, bad, seen>>

EraseWrite ==
  /\ xst = "read" /\ xage <= Hold
  /\ IF xres = {}
       THEN /\ xout' = {} /\ UNCHANGED boxv
       ELSE /\ Free # {} /\ xout' = {NewBox} /\ Alloc(NewBox, xres)
  /\ xst' = "wrote"
  /\ UNCHANGED <<logv, mf, fv, cv, xin, xres, xage, rv, kv, crashes, bad, seen>>

EraseCommit ==
  /\ xst = "wrote" /\ xage <= Hold /\ xin \subseteq mf.boxes
  /\ mf' = [mf EXCEPT !.boxes = (@ \ xin) \cup xout]
  /\ Touch(xin)
  /\ EnterMf(xout)
  /\ XIdle
  /\ UNCHANGED <<logv, boxSt, content, fv, cv, rv, kv, crashes, bad, seen>>

EraseFail ==
  /\ xst = "wrote" /\ xage <= Hold /\ ~(xin \subseteq mf.boxes)
  /\ XIdle
  /\ UNCHANGED <<logv, boxv, mf, fv, cv, rv, kv, crashes, bad, seen>>

EraseStop(crash) ==
  /\ xst # "idle"
  /\ IF crash THEN crashes < MaxCrash /\ crashes' = crashes + 1
              ELSE xage > Hold /\ UNCHANGED crashes
  /\ XIdle
  /\ IF crash /\ xout # {} THEN Mark("crashConOrfano") ELSE UNCHANGED seen
  /\ UNCHANGED <<logv, boxv, mf, fv, cv, rv, kv, bad>>

\* fine della cancellazione fisica: nel foglio non resta niente di vecchio del cliente
EraseDone ==
  /\ \E x \in mf.ledger :
       /\ ~\E b \in mf.boxes : \E e \in content[b] : e.c = x[1] /\ e.s < x[2]
       /\ mf' = [mf EXCEPT !.ledger = @ \ {x}]
  /\ Mark("cancellazioneFinita")
  /\ UNCHANGED <<logv, boxv, fv, cv, xv, rv, kv, crashes, bad>>

-----------------------------------------------------------------------------
(* Lettori con istantanea: indice e documento in due momenti.               *)

RIdle(r) ==
  /\ rst' = [rst EXCEPT ![r] = "idle"]
  /\ rB' = [rB EXCEPT ![r] = {}]
  /\ rF' = [rF EXCEPT ![r] = 0]
  /\ rS' = [rS EXCEPT ![r] = 0]
  /\ rG' = [rG EXCEPT ![r] = {}]
  /\ rc' = [rc EXCEPT ![r] = "-"]
  /\ rk' = [rk EXCEPT ![r] = "-"]
  /\ ridx' = [ridx EXCEPT ![r] = {}]
  /\ rage' = [rage EXCEPT ![r] = 0]

ReadStart(r) ==
  /\ rst[r] = "idle"
  /\ \E c \in Clients, k \in Colls :
       /\ rst' = [rst EXCEPT ![r] = "snap"]
       /\ rB' = [rB EXCEPT ![r] = mf.boxes]
       /\ rF' = [rF EXCEPT ![r] = mf.flushed]
       /\ rS' = [rS EXCEPT ![r] = seqNow]
       /\ rG' = [rG EXCEPT ![r] = mf.ledger]
       /\ rc' = [rc EXCEPT ![r] = c]
       /\ rk' = [rk EXCEPT ![r] = k]
       /\ ridx' = [ridx EXCEPT ![r] = {}]
       /\ rage' = [rage EXCEPT ![r] = 0]
  /\ UNCHANGED <<logv, boxv, mf, fv, cv, xv, kv, crashes, bad, seen>>

ReadIdx(r) ==
  /\ rst[r] = "snap" /\ rage[r] <= RHold
  /\ IF ~(rB[r] \subseteq Live)
       THEN /\ bad' = bad \cup {"fail"} /\ RIdle(r)
       ELSE /\ ridx' = [ridx EXCEPT ![r] = IdxSet(rB[r], rF[r], rS[r], rG[r], rc[r], rk[r])]
            /\ rst' = [rst EXCEPT ![r] = "idx"]
            /\ UNCHANGED <<rB, rF, rS, rG, rc, rk, rage, bad>>
  /\ UNCHANGED <<logv, boxv, mf, fv, cv, xv, kv, crashes, seen>>

ReadDoc(r) ==
  /\ rst[r] = "idx" /\ rage[r] <= RHold
  /\ LET B == IF ReaderTwoSnap THEN mf.boxes ELSE rB[r]
         F == IF ReaderTwoSnap THEN mf.flushed ELSE rF[r]
         S == IF ReaderTwoSnap THEN seqNow ELSE rS[r]
         G == IF ReaderTwoSnap THEN mf.ledger ELSE rG[r]
         d == DocVal(B, F, S, G, rc[r], rk[r])
         t == tr[rS[r]][<<rc[r], rk[r]>>]
     IN IF ~(B \subseteq Live)
          THEN bad' = bad \cup {"fail"}
          ELSE bad' = bad \cup (IF \E v \in ridx[r] : v # d THEN {"idx"} ELSE {})
                          \cup (IF d # 0 /\ d \notin ridx[r] THEN {"doc"} ELSE {})
                          \cup (IF d # t \/ ridx[r] # Exp(t) THEN {"ver"} ELSE {})
  /\ RIdle(r)
  /\ IF rB[r] # mf.boxes THEN Mark("lettoreDuranteScambio") ELSE UNCHANGED seen
  /\ UNCHANGED <<logv, boxv, mf, fv, cv, xv, kv, crashes>>

\* tetto di tempo del lettore: oltre RHold giri smette e ricomincia
ReadTimeout(r) ==
  /\ rst[r] # "idle" /\ rage[r] > RHold
  /\ RIdle(r)
  /\ UNCHANGED <<logv, boxv, mf, fv, cv, xv, kv, crashes, bad, seen>>

-----------------------------------------------------------------------------
(* Il ramo (fork): legge foglio + coda del quaderno + lista dei cancellati, *)
(* mette la spilla entro FHold giri (DECISIONS 2026-11-02).                 *)

KIdle ==
  /\ kst' = "idle" /\ kB' = {} /\ kF' = 0 /\ kS' = 0 /\ kG' = {} /\ kL' = {} /\ kX' = {}
  /\ kage' = 0

ForkRead ==
  /\ kst = "idle" /\ nForks < MaxForks
  /\ kst' = "read" /\ kB' = mf.boxes /\ kF' = mf.flushed /\ kS' = seqNow
  /\ kG' = IF ForkNoLedger THEN {} ELSE mf.ledger
  /\ kL' = LogEnt(mf.flushed, seqNow)   \* il ramo copia la coda del quaderno
  /\ kX' = LogX(mf.flushed, seqNow)
  /\ kage' = 0
  /\ UNCHANGED <<logv, boxv, mf, fv, cv, xv, rv, nForks, crashes, bad, seen>>

ForkPin ==
  /\ kst = "read" /\ kage <= FHold
  /\ kst' = "live" /\ nForks' = nForks + 1
  /\ IF \E q \in 1..kS : hist[q].t = "x" THEN Mark("ramoDopoCancellazione") ELSE UNCHANGED seen
  /\ UNCHANGED <<logv, boxv, mf, fv, cv, xv, rv, kB, kF, kS, kG, kL, kX, kage, crashes, bad>>

ForkTimeout ==
  /\ kst = "read" /\ kage > FHold
  /\ KIdle
  /\ UNCHANGED <<logv, boxv, mf, fv, cv, xv, rv, nForks, crashes, bad, seen>>

RamoDrop ==
  /\ kst = "live"
  /\ KIdle
  /\ Touch(kB)
  /\ UNCHANGED <<logv, boxSt, content, wasIn, mf, fv, cv, xv, rv, nForks, crashes, bad, seen>>

-----------------------------------------------------------------------------
(* La pulizia (GC): un giro. Cancella le scatole fuori dal foglio e dalla   *)
(* spilla con eta' >= Grace, poi fa invecchiare tutto di 1.                 *)

Gc ==
  LET inUse == IF GcNoPins THEN mf.boxes ELSE mf.boxes \cup PinB
      del   == {b \in Boxes : boxSt[b] = "live" /\ b \notin inUse /\ bage[b] >= Grace
                              /\ (~GcOnlyRemoved \/ wasIn[b])}
  IN /\ boxSt' = [b \in Boxes |-> IF b \in del THEN "gone" ELSE boxSt[b]]
     /\ content' = [b \in Boxes |-> IF b \in del THEN {} ELSE content[b]]
     /\ wasIn' = [b \in Boxes |-> IF b \in del THEN FALSE ELSE wasIn[b]]
     /\ bage' = [b \in Boxes |-> IF boxSt[b] = "live" /\ b \notin del /\ b \notin inUse
                                 THEN Min(bage[b] + 1, Grace) ELSE 0]
     /\ fage' = IF fst # "idle" THEN Min(fage + 1, Hold + 1) ELSE 0
     /\ cage' = [w \in Workers |-> IF cst[w] # "idle" THEN Min(cage[w] + 1, Hold + 1) ELSE 0]
     /\ xage' = IF xst # "idle" THEN Min(xage + 1, Hold + 1) ELSE 0
     /\ rage' = [r \in Readers |-> IF rst[r] # "idle" THEN Min(rage[r] + 1, RHold + 1) ELSE 0]
     /\ kage' = IF kst = "read" THEN Min(kage + 1, FHold + 1) ELSE 0
     \* il quaderno tronca con un giro di ritardo: tiene la coda per i lettori in corso
     /\ trim' = Max(trim, pf)
     /\ pf' = mf.flushed
     /\ IF Track /\ \E b \in del : ~wasIn[b] THEN seen' = seen \cup {"orfanoPulito"}
        ELSE UNCHANGED seen
  /\ UNCHANGED <<seqNow, hist, logE, tr, mf, fst, fbox, fFrom, fTo, fX, cst, cin, cres, cout, cM,
                 xst, xin, xres, xout, rst, rB, rF, rS, rG, rc, rk, ridx,
                 kst, kB, kF, kS, kG, kL, kX, nForks, crashes, bad>>

-----------------------------------------------------------------------------
Next ==
  \/ \E c \in Clients, k \in Colls, v \in Vals \cup {0} : Write(c, k, v)
  \/ \E c \in Clients : Erase(c)
  \/ FlushWrite \/ FlushCommit \/ FlushStop(TRUE) \/ FlushStop(FALSE)
  \/ \E w \in Workers :
       \/ CompactRead(w) \/ CompactWrite(w) \/ CompactCommit(w) \/ CompactCommit2(w)
       \/ CompactFail(w) \/ CompactStop(w, TRUE) \/ CompactStop(w, FALSE)
  \/ EraseRead \/ EraseWrite \/ EraseCommit \/ EraseFail \/ EraseStop(TRUE) \/ EraseStop(FALSE)
  \/ EraseDone
  \/ \E r \in Readers : ReadStart(r) \/ ReadIdx(r) \/ ReadDoc(r) \/ ReadTimeout(r)
  \/ ForkRead \/ ForkPin \/ ForkTimeout \/ RamoDrop
  \/ Gc

Spec == Init /\ [][Next]_vars

\* equita': chi puo' lavorare prima o poi lavora; le scritture e i lettori no (sono il cliente)
Fair ==
  /\ SF_vars(FlushWrite) /\ SF_vars(FlushCommit) /\ WF_vars(FlushStop(FALSE))
  /\ \A w \in Workers :
       /\ SF_vars(CompactRead(w)) /\ SF_vars(CompactWrite(w)) /\ SF_vars(CompactCommit(w))
       /\ WF_vars(CompactCommit2(w)) /\ WF_vars(CompactFail(w)) /\ WF_vars(CompactStop(w, FALSE))
  /\ SF_vars(EraseRead) /\ SF_vars(EraseWrite) /\ SF_vars(EraseCommit)
  /\ WF_vars(EraseFail) /\ WF_vars(EraseStop(FALSE)) /\ WF_vars(EraseDone)
  /\ \A r \in Readers : WF_vars(ReadTimeout(r))
  /\ WF_vars(ForkTimeout)
  /\ WF_vars(Gc)

LiveSpec == Spec /\ Fair

-----------------------------------------------------------------------------
(* Proprieta' di sicurezza (invarianti).                                    *)

NessunDatoPerso ==
  \A x \in Keys :
     /\ DocVal(mf.boxes, mf.flushed, seqNow, mf.ledger, x[1], x[2]) = tr[seqNow][x]
     /\ IdxSet(mf.boxes, mf.flushed, seqNow, mf.ledger, x[1], x[2]) = Exp(tr[seqNow][x])

FoglioIntegro == mf.boxes \subseteq Live

IndicePuntaDoc == "idx" \notin bad
DocHaIndice    == "doc" \notin bad
UnaVersione    == "ver" \notin bad
LettoreMaiSenzaScatola == "fail" \notin bad

RamoCorretto ==
  kst = "live" =>
    \A x \in Keys :
      /\ DocValV(kB, kL, kG \cup kX, x[1], x[2]) = tr[kS][x]
      /\ IdxSetV(kB, kL, kG \cup kX, x[1], x[2]) = Exp(tr[kS][x])

RamoIntegro == (kst = "live" \/ (kst = "read" /\ kage <= FHold)) => kB \subseteq Live

(* Proprieta' di vivezza.                                                   *)

InFlight == UNION {cout[w] : w \in Workers} \cup xout \cup (IF fbox # 0 THEN {fbox} ELSE {})
Orphan(b) == boxSt[b] = "live" /\ b \notin mf.boxes \cup PinB \cup InFlight
             /\ ~(kst = "read" /\ b \in kB)

\* le scritture finiscono (MaxSeq) e i crash pure (MaxCrash): allora prima o poi non resta
\* nessuna scatola orfana, ne' dei crash ne' tolta dal foglio, e cosi' per sempre
NessunOrfanoPerSempre == <>[](\A b \in Boxes : ~Orphan(b))
PileSottoIlTetto == <>[](\A c \in Clients : Piles(c) <= Cap)
CancellazioneFinita == <>[](mf.ledger = {})

(* Raggiungibilita' (con Track): ognuna DEVE essere violata, cosi' si vede  *)
(* che il caso succede davvero nel modello.                                 *)
MaiLavaggioFallitoPerCancellazione == "lavaggioFallitoPerCancellazione" \notin seen
MaiRamoDopoCancellazione == "ramoDopoCancellazione" \notin seen
MaiCrashConOrfano == "crashConOrfano" \notin seen
MaiOrfanoPulito == "orfanoPulito" \notin seen
MaiLettoreDuranteScambio == "lettoreDuranteScambio" \notin seen
MaiCancellazioneFinita == "cancellazioneFinita" \notin seen
MaiLapideTolta == "lapideTolta" \notin seen
MaiRicreatoDopoCancellazione == "ricreatoDopoCancellazione" \notin seen
MaiScambioConLettore == "scambioConLettore" \notin seen
=============================================================================
