---------------------------- MODULE ValidatingLog ----------------------------
(***************************************************************************)
(* Il registro "senza cuoco" (docs/protocollo.md): un log a 3 repliche con  *)
(* un leader che VALIDA le scritture prima di ordinarle.                    *)
(*                                                                          *)
(* Nucleo di replicazione: Raft semplificato.                               *)
(*  - Elect(c, Q): elezione atomica di c con i voti della maggioranza Q;    *)
(*    epoca nuova = 1 + massima epoca esistente; il log di c deve essere    *)
(*    almeno aggiornato quanto quello di ogni votante (restrizione di Raft);*)
(*    il nuovo leader aggiunge subito la barriera e RICOSTRUISCE le mappe   *)
(*    di validazione e la tabella degli id dal proprio log (dalla finestra).*)
(*  - Replicate(l, f): il leader porta f in pari di una voce dopo il punto  *)
(*    in cui i due log combaciano (controllo del prefisso di Raft, voci in  *)
(*    conflitto troncate); f adotta l'epoca e impara la confermata.         *)
(*  - AdvanceCommit(l): confermata = posizione piu' alta con maggioranza di  *)
(*    repliche che combaciano E voce dell'epoca corrente (regola di Raft:   *)
(*    e' per questo che serve la barriera).                                 *)
(* Una replica "persa" (Lose) non agisce piu' e non vota piu'.               *)
(*                                                                          *)
(* Validazione (docs/tla-registro-decisioni.md, sezione A, scelta c):      *)
(* ogni transazione t ha dipendenze Deps[t] = insieme di [k, mode] con mode *)
(* "esiste" o "valore", e scritture Writes[t] = insieme di [k, kind] con    *)
(* kind "delta" o "strutturale". Il leader tiene per chiave l'ultima        *)
(* posizione strutturale (mapS) e l'ultima posizione qualsiasi (mapA).      *)
(* Un commit preparato a S e' rifiutato se una dipendenza (k, esiste) ha    *)
(* mapS[k] > S o una dipendenza (k, valore) ha mapA[k] > S, oppure se S e'  *)
(* sotto l'inizio della finestra (wstart) o sopra l'ultima voce del leader. *)
(* Un id gia' nella tabella non produce una seconda voce (dedup).           *)
(*                                                                          *)
(* Controlli negativi (costanti; con tutte FALSE ogni invariante deve       *)
(* valere, con una TRUE TLC deve trovare un controesempio):                 *)
(*  NoValidate        il leader non valida                                  *)
(*  NoRebuild         il nuovo leader parte con mappe e tabella id vuote    *)
(*  AcceptOldS        il leader accetta S sotto l'inizio della finestra     *)
(*  AckOnOne          conferma con 1 replica su 3                           *)
(*  NoVoteRestriction si vota anche un candidato con il log indietro        *)
(*  RetryBeyondWindow il nodo d'ingresso ritenta un id che il leader ha     *)
(*                    gia' dimenticato (oltre la finestra)                  *)
(***************************************************************************)
EXTENDS Naturals, Sequences, FiniteSets, TLC

CONSTANTS Replicas, Keys, Txns, Deps, Writes,
          MaxEpoch, MaxAttempts, MaxLoss,
          NoValidate, NoRebuild, AcceptOldS, AckOnOne, NoVoteRestriction, RetryBeyondWindow

Barrier == "barriera"
Majority == IF AckOnOne THEN 1 ELSE (Cardinality(Replicas) \div 2) + 1

VARIABLES
  epoch,    \* replica -> epoca piu' alta vista
  role,     \* replica -> "leader" | "follower"
  alive,    \* replica -> BOOLEAN (FALSE = disco perso, non agisce piu')
  log,      \* replica -> sequenza di voci [ep, tx, s]
  commit,   \* replica -> posizione confermata nota alla replica
  match,    \* leader -> replica -> posizione fino alla quale il log della replica combacia col suo
  wstart,   \* replica -> inizio della finestra (prima posizione ancora in RAM / nelle mappe)
  mapS,     \* leader -> chiave -> ultima posizione di una scrittura strutturale nella finestra (0 = nessuna)
  mapA,     \* leader -> chiave -> ultima posizione di una scrittura qualsiasi nella finestra
  ids,      \* leader -> transazione -> posizione nella finestra (0 = sconosciuta)
  attempts, \* transazione -> numero di COMMIT inviati (limite del modello)
  outcome,  \* transazione -> insieme delle posizioni restituite al client come CONFERMATO
  cmtLog,   \* fantasma: il prefisso confermato piu' lungo mai osservato
  losses    \* numero di repliche perse

vars == <<epoch, role, alive, log, commit, match, wstart, mapS, mapA, ids, attempts, outcome, cmtLog, losses>>

Entry(e, t, s) == [ep |-> e, tx |-> t, s |-> s]
Max(S) == CHOOSE x \in S : \A y \in S : y <= x
Min(a, b) == IF a < b THEN a ELSE b
EmptyKeyMap == [k \in Keys |-> 0]
EmptyIds == [t \in Txns |-> 0]

WritesOf(en) == IF en.tx = Barrier THEN {} ELSE Writes[en.tx]

\* mappe e tabella id come le ricostruirebbe un leader dal proprio log, dalla posizione ws in su
LastW(lg, ws, k, kinds) ==
  LET P == {i \in ws..Len(lg) : \E w \in WritesOf(lg[i]) : w.k = k /\ w.kind \in kinds}
  IN IF P = {} THEN 0 ELSE Max(P)
DerivedMapS(lg, ws) == [k \in Keys |-> LastW(lg, ws, k, {"strutturale"})]
DerivedMapA(lg, ws) == [k \in Keys |-> LastW(lg, ws, k, {"delta", "strutturale"})]
DerivedIds(lg, ws) ==
  [t \in Txns |-> LET P == {i \in ws..Len(lg) : lg[i].tx = t} IN IF P = {} THEN 0 ELSE Max(P)]

LastEpoch(lg) == IF Len(lg) = 0 THEN 0 ELSE lg[Len(lg)].ep
\* il log di c e' almeno aggiornato quanto quello di v (restrizione del voto di Raft)
UpToDate(c, v) ==
  \/ LastEpoch(log[c]) > LastEpoch(log[v])
  \/ (LastEpoch(log[c]) = LastEpoch(log[v]) /\ Len(log[c]) >= Len(log[v]))

MaxEpochNow == Max({epoch[r] : r \in Replicas})
MaxCommit == Max({commit[r] : r \in Replicas})
Alive == {r \in Replicas : alive[r]}

\* lunghezza del prefisso comune
MatchLen(a, b) == Max({i \in 0..Min(Len(a), Len(b)) : SubSeq(a, 1, i) = SubSeq(b, 1, i)})

Init ==
  /\ epoch = [r \in Replicas |-> 0]
  /\ role = [r \in Replicas |-> "follower"]
  /\ alive = [r \in Replicas |-> TRUE]
  /\ log = [r \in Replicas |-> << >>]
  /\ commit = [r \in Replicas |-> 0]
  /\ match = [r \in Replicas |-> [q \in Replicas |-> 0]]
  /\ wstart = [r \in Replicas |-> 1]
  /\ mapS = [r \in Replicas |-> EmptyKeyMap]
  /\ mapA = [r \in Replicas |-> EmptyKeyMap]
  /\ ids = [r \in Replicas |-> EmptyIds]
  /\ attempts = [t \in Txns |-> 0]
  /\ outcome = [t \in Txns |-> {}]
  /\ cmtLog = << >>
  /\ losses = 0

(* ------------------------------------------------------- cambio di leader *)

Elect(c, Q) ==
  /\ alive[c] /\ c \in Q /\ Q \subseteq Alive /\ Cardinality(Q) * 2 > Cardinality(Replicas)
  /\ ~(role[c] = "leader" /\ epoch[c] = MaxEpochNow)   \* una rielezione dello stesso leader non aggiunge nulla
  /\ MaxEpochNow + 1 <= MaxEpoch
  /\ NoVoteRestriction \/ \A v \in Q : UpToDate(c, v)
  /\ LET t == MaxEpochNow + 1
         newLog == Append(log[c], Entry(t, Barrier, 0))
     IN /\ epoch' = [r \in Replicas |-> IF r \in Q THEN t ELSE epoch[r]]
        /\ role' = [r \in Replicas |-> IF r = c THEN "leader" ELSE IF r \in Q THEN "follower" ELSE role[r]]
        /\ log' = [log EXCEPT ![c] = newLog]
        /\ match' = [match EXCEPT ![c] = [r \in Replicas |-> IF r = c THEN Len(newLog) ELSE 0]]
        /\ mapS' = [mapS EXCEPT ![c] = IF NoRebuild THEN EmptyKeyMap ELSE DerivedMapS(newLog, wstart[c])]
        /\ mapA' = [mapA EXCEPT ![c] = IF NoRebuild THEN EmptyKeyMap ELSE DerivedMapA(newLog, wstart[c])]
        /\ ids' = [ids EXCEPT ![c] = IF NoRebuild THEN EmptyIds ELSE DerivedIds(newLog, wstart[c])]
  /\ UNCHANGED <<alive, commit, wstart, attempts, outcome, cmtLog, losses>>

\* il leader e' pronto quando la barriera del proprio mandato e' confermata
LeaderReady(l) == \E i \in 1..commit[l] : log[l][i].ep = epoch[l]

(* -------------------------------------------------------------- scritture *)

Conflict(l, t, s) ==
  \E d \in Deps[t] :
    \/ (d.mode = "esiste" /\ mapS[l][d.k] > s)
    \/ (d.mode = "valore" /\ mapA[l][d.k] > s)

\* vincolo del modello (non del protocollo): il nodo d'ingresso ritenta un id solo finche'
\* la sua eventuale voce e' ancora nella finestra del leader
RetryWithinWindow(l, t) == \A i \in 1..Len(log[l]) : log[l][i].tx = t => i >= wstart[l]

Submit(l, t) ==
  /\ alive[l] /\ role[l] = "leader" /\ LeaderReady(l)
  /\ attempts[t] < MaxAttempts
  /\ RetryBeyondWindow \/ RetryWithinWindow(l, t)
  /\ attempts' = [attempts EXCEPT ![t] = @ + 1]
  /\ IF ids[l][t] /= 0
     THEN \* id gia' visto: stesso esito, nessuna voce nuova
          /\ outcome' = IF ids[l][t] <= commit[l] THEN [outcome EXCEPT ![t] = @ \cup {ids[l][t]}] ELSE outcome
          /\ UNCHANGED <<log, match, mapS, mapA, ids>>
     ELSE \E s \in 0..MaxCommit :   \* S: una posizione confermata letta dal nodo d'ingresso
          IF \/ (s < wstart[l] /\ ~AcceptOldS)      \* S troppo vecchia: non si puo' validare
             \/ s > Len(log[l])                      \* S sconosciuta: il leader e' deposto
             \/ (~NoValidate /\ Conflict(l, t, s))   \* conflitto
          THEN UNCHANGED <<log, match, mapS, mapA, ids, outcome>>
          ELSE LET c == Len(log[l]) + 1
                   ws == Writes[t]
               IN /\ log' = [log EXCEPT ![l] = Append(@, Entry(epoch[l], t, s))]
                  /\ match' = [match EXCEPT ![l][l] = c]
                  /\ mapS' = [mapS EXCEPT ![l] = [k \in Keys |-> IF \E w \in ws : w.k = k /\ w.kind = "strutturale" THEN c ELSE @[k]]]
                  /\ mapA' = [mapA EXCEPT ![l] = [k \in Keys |-> IF \E w \in ws : w.k = k THEN c ELSE @[k]]]
                  /\ ids' = [ids EXCEPT ![l][t] = c]
                  /\ UNCHANGED outcome
  /\ UNCHANGED <<epoch, role, alive, commit, wstart, cmtLog, losses>>

(* ----------------------------------------------------------- replicazione *)

Replicate(l, f) ==
  /\ alive[l] /\ alive[f] /\ l /= f /\ role[l] = "leader" /\ epoch[f] <= epoch[l]
  /\ LET m == MatchLen(log[l], log[f])
     IN /\ m < Len(log[l])
        /\ log' = [log EXCEPT ![f] = SubSeq(log[l], 1, m + 1)]
        /\ match' = [match EXCEPT ![l][f] = m + 1]
        /\ commit' = [commit EXCEPT ![f] = Max({commit[f], Min(commit[l], m + 1)})]
  /\ epoch' = [epoch EXCEPT ![f] = epoch[l]]
  /\ role' = [role EXCEPT ![f] = "follower"]
  /\ UNCHANGED <<alive, wstart, mapS, mapA, ids, attempts, outcome, cmtLog, losses>>

\* heartbeat: nessuna voce nuova, il seguace impara la confermata (solo sul prefisso che combacia)
Heartbeat(l, f) ==
  /\ alive[l] /\ alive[f] /\ l /= f /\ role[l] = "leader" /\ epoch[f] <= epoch[l]
  /\ LET m == MatchLen(log[l], log[f])
         c == Min(commit[l], m)
     IN /\ c > commit[f]
        /\ commit' = [commit EXCEPT ![f] = c]
  /\ epoch' = [epoch EXCEPT ![f] = epoch[l]]
  /\ role' = [role EXCEPT ![f] = "follower"]
  /\ UNCHANGED <<alive, log, match, wstart, mapS, mapA, ids, attempts, outcome, cmtLog, losses>>

AdvanceCommit(l) ==
  /\ alive[l] /\ role[l] = "leader"
  /\ LET agreeing(i) == Cardinality({r \in Replicas : match[l][r] >= i}) >= Majority
         cands == {i \in (commit[l] + 1)..Len(log[l]) : agreeing(i) /\ log[l][i].ep = epoch[l]}
     IN /\ cands /= {}
        /\ LET c == Max(cands)
           IN /\ commit' = [commit EXCEPT ![l] = c]
              /\ cmtLog' = IF c > Len(cmtLog) THEN SubSeq(log[l], 1, c) ELSE cmtLog
              /\ outcome' = [t \in Txns |-> outcome[t] \cup {i \in (commit[l] + 1)..c : log[l][i].tx = t}]
  /\ UNCHANGED <<epoch, role, alive, log, match, wstart, mapS, mapA, ids, attempts, losses>>

(* ------------------------------------------------------- finestra e guasti *)

\* la finestra avanza di una posizione (solo su voci confermate: una voce vecchia di 60 s e' confermata);
\* un leader dimentica chiavi e id usciti dalla finestra
AdvanceWindow(s) ==
  /\ alive[s] /\ wstart[s] <= commit[s]
  /\ LET nw == wstart[s] + 1
     IN /\ wstart' = [wstart EXCEPT ![s] = nw]
        /\ IF role[s] = "leader"
           THEN /\ mapS' = [mapS EXCEPT ![s] = [k \in Keys |-> IF @[k] < nw THEN 0 ELSE @[k]]]
                /\ mapA' = [mapA EXCEPT ![s] = [k \in Keys |-> IF @[k] < nw THEN 0 ELSE @[k]]]
                /\ ids' = [ids EXCEPT ![s] = [t \in Txns |-> IF @[t] < nw THEN 0 ELSE @[t]]]
           ELSE UNCHANGED <<mapS, mapA, ids>>
  /\ UNCHANGED <<epoch, role, alive, log, commit, match, attempts, outcome, cmtLog, losses>>

Lose(r) ==
  /\ alive[r] /\ losses < MaxLoss
  /\ alive' = [alive EXCEPT ![r] = FALSE]
  /\ losses' = losses + 1
  /\ UNCHANGED <<epoch, role, log, commit, match, wstart, mapS, mapA, ids, attempts, outcome, cmtLog>>

Next ==
  \/ \E c \in Replicas, Q \in SUBSET Replicas : Elect(c, Q)
  \/ \E l \in Replicas, t \in Txns : Submit(l, t)
  \/ \E l, f \in Replicas : Replicate(l, f) \/ Heartbeat(l, f)
  \/ \E l \in Replicas : AdvanceCommit(l) \/ AdvanceWindow(l) \/ Lose(l)

Spec == Init /\ [][Next]_vars

(* ------------------------------------------------------------- invarianti *)

TypeOK ==
  /\ \A r \in Replicas : epoch[r] \in 0..MaxEpoch /\ role[r] \in {"leader", "follower"}
                        /\ commit[r] <= Len(log[r]) /\ wstart[r] \in 1..(Len(log[r]) + 1)
  /\ losses <= MaxLoss

\* (1) nessuna coppia di record confermati in conflitto: se b (prima) scrive una chiave da cui
\*     a (dopo) dipende, nel modo che conta, allora b e' prima dell'istantanea di a.
NoConflictingCommits ==
  \A i, j \in 1..Len(cmtLog) :
    (i < j /\ cmtLog[i].tx /= Barrier /\ cmtLog[j].tx /= Barrier) =>
      \A d \in Deps[cmtLog[j].tx], w \in Writes[cmtLog[i].tx] :
        (d.k = w.k /\ (d.mode = "valore" \/ w.kind = "strutturale")) => i <= cmtLog[j].s

\* (2) ordine totale stabile: il prefisso confermato di ogni replica e' un prefisso di cmtLog
StableOrder ==
  \A r \in Replicas : commit[r] <= Len(cmtLog) /\ SubSeq(log[r], 1, commit[r]) = SubSeq(cmtLog, 1, commit[r])

\* (3) un cambio di leader non duplica: ogni transazione compare al piu' una volta tra i confermati,
\*     e il client riceve al piu' un esito, che corrisponde alla voce confermata
NoDuplicateIds ==
  \A i, j \in 1..Len(cmtLog) : (i /= j /\ cmtLog[i].tx /= Barrier) => cmtLog[i].tx /= cmtLog[j].tx
OneOutcome ==
  \A t \in Txns : Cardinality(outcome[t]) <= 1 /\ \A p \in outcome[t] : p <= Len(cmtLog) /\ cmtLog[p].tx = t

\* (4) la posizione confermata non torna indietro (proprieta' di azione)
CommitMonotonic ==
  [][ /\ \A r \in Replicas : commit'[r] >= commit[r]
      /\ Len(cmtLog') >= Len(cmtLog) /\ SubSeq(cmtLog', 1, Len(cmtLog)) = cmtLog ]_vars

\* (5) un record confermato sopravvive alla perdita di una replica: e' nel log di una replica viva
\*     e nel log del leader dell'epoca piu' alta
Durability ==
  \A i \in 1..Len(cmtLog) : \E r \in Alive : Len(log[r]) >= i /\ log[r][i] = cmtLog[i]
LeaderCompleteness ==
  \A l \in Alive : (role[l] = "leader" /\ epoch[l] = MaxEpochNow) =>
    Len(log[l]) >= Len(cmtLog) /\ SubSeq(log[l], 1, Len(cmtLog)) = cmtLog

\* (6) le mappe e la tabella id di ogni leader sono quelle che si ricaverebbero dal suo log
MapsCorrect ==
  \A l \in Alive : role[l] = "leader" =>
    /\ mapS[l] = DerivedMapS(log[l], wstart[l])
    /\ mapA[l] = DerivedMapA(log[l], wstart[l])
    /\ ids[l] = DerivedIds(log[l], wstart[l])

=============================================================================
