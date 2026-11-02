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
(*  - Replicate(l, f, p, n): APPEND con posPrecedente p e n voci (0 = il    *)
(*    battito); controllo del prefisso di Raft, troncamento solo dal primo  *)
(*    conflitto; f adotta l'epoca e impara min(confermata, p + n).          *)
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
(*  Giro 2 (revisore di S1):                                               *)
(*  CommitByTail      il seguace prende come confermata min(confermata del  *)
(*                    capo, sua ultima posizione) invece della regola di    *)
(*                    Raft min(confermata, posPrecedente + voci ricevute)   *)
(*  ValidateCommittedOnly il capo valida solo contro le voci confermate     *)
(*  RebuildCommittedOnly  il nuovo capo ricostruisce mappe e tabella id     *)
(*                    solo dalla parte confermata del proprio log           *)
(*  CommitOldEpoch    il capo conferma anche voci di epoche precedenti      *)
(*                    (figura 8 di Raft: serve un modello con 4 epoche)     *)
(* Opzioni del modello (non controlli negativi):                            *)
(*  OldPrev           APPEND con posPrecedente qualsiasi sotto il punto in  *)
(*                    cui i log combaciano (battito partito prima delle     *)
(*                    voci, posPrecedente abbassato dopo un rifiuto): il    *)
(*                    seguace tronca solo dal primo conflitto e tiene la    *)
(*                    propria coda se non c'e' conflitto, come Raft         *)
(*  FollowerWindow    anche la finestra dei seguaci avanza                  *)
(* Passo 5 (sessioni): con SessionLookup = TRUE ogni scrittura scrive anche *)
(* la chiave di sessione con il proprio esito, e il nodo d'ingresso, prima  *)
(* di inviare un tentativo, legge quella chiave a S: se l'id e' gia' li'    *)
(* restituisce l'esito memorizzato senza inviare nulla. Nel modello la      *)
(* lettura a S della chiave di sessione e' "t compare in cmtLog entro S".   *)
(***************************************************************************)
EXTENDS Naturals, Sequences, FiniteSets, TLC

CONSTANTS Replicas, Keys, Txns, Deps, Writes,
          MaxEpoch, MaxAttempts, MaxLoss,
          NoValidate, NoRebuild, AcceptOldS, AckOnOne, NoVoteRestriction, RetryBeyondWindow,
          SessionLookup,
          CommitByTail, ValidateCommittedOnly, RebuildCommittedOnly, CommitOldEpoch,
          OldPrev, FollowerWindow

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
  attempts, \* transazione -> numero di voci scritte per la transazione (limite del modello)
  outcome,  \* transazione -> insieme delle posizioni restituite al client come CONFERMATO
  cmtLog,   \* fantasma: il prefisso confermato piu' lungo mai osservato
  losses,   \* numero di repliche perse
  diskStart \* replica -> inizioDisco: prima posizione ancora sul disco locale. Le voci sotto restano
            \* nella sequenza log[r] solo come (posizione, epoca) per il controllo del prefisso e il
            \* voto (come lo snapshot di Raft): nessuna azione le serve. In questo modulo resta 1;
            \* la alzano lo scarico e la ricarica da S3 di LettureScarico.

vars == <<epoch, role, alive, log, commit, match, wstart, mapS, mapA, ids, attempts, outcome, cmtLog, losses, diskStart>>

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
MaxLog == Max({Len(log[r]) : r \in Replicas})
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
  /\ diskStart = [r \in Replicas |-> 1]

(* ------------------------------------------------------- cambio di leader *)

Elect(c, Q) ==
  /\ alive[c] /\ c \in Q /\ Q \subseteq Alive /\ Cardinality(Q) * 2 > Cardinality(Replicas)
  /\ ~(role[c] = "leader" /\ epoch[c] = MaxEpochNow)   \* una rielezione dello stesso leader non aggiunge nulla
  /\ MaxEpochNow + 1 <= MaxEpoch
  /\ NoVoteRestriction \/ \A v \in Q : UpToDate(c, v)
  /\ LET t == MaxEpochNow + 1
         newLog == Append(log[c], Entry(t, Barrier, 0))
         \* RebuildCommittedOnly: il nuovo capo ricostruisce solo dalla parte confermata (sbagliato)
         src == IF RebuildCommittedOnly THEN SubSeq(newLog, 1, commit[c]) ELSE newLog
     IN /\ epoch' = [r \in Replicas |-> IF r \in Q THEN t ELSE epoch[r]]
        /\ role' = [r \in Replicas |-> IF r = c THEN "leader" ELSE IF r \in Q THEN "follower" ELSE role[r]]
        /\ log' = [log EXCEPT ![c] = newLog]
        /\ match' = [match EXCEPT ![c] = [r \in Replicas |-> IF r = c THEN Len(newLog) ELSE 0]]
        /\ mapS' = [mapS EXCEPT ![c] = IF NoRebuild THEN EmptyKeyMap ELSE DerivedMapS(src, wstart[c])]
        /\ mapA' = [mapA EXCEPT ![c] = IF NoRebuild THEN EmptyKeyMap ELSE DerivedMapA(src, wstart[c])]
        /\ ids' = [ids EXCEPT ![c] = IF NoRebuild THEN EmptyIds ELSE DerivedIds(src, wstart[c])]
  /\ UNCHANGED <<alive, commit, wstart, attempts, outcome, cmtLog, losses, diskStart>>

\* il leader e' pronto quando la barriera del proprio mandato e' confermata
LeaderReady(l) == \E i \in 1..commit[l] : log[l][i].ep = epoch[l]

(* -------------------------------------------------------------- scritture *)

\* ValidateCommittedOnly: il capo guarda solo le voci gia' confermate (sbagliato: due commit in
\* conflitto nello stesso lotto non si vedono)
VMapS(l) == IF ValidateCommittedOnly THEN DerivedMapS(SubSeq(log[l], 1, commit[l]), wstart[l]) ELSE mapS[l]
VMapA(l) == IF ValidateCommittedOnly THEN DerivedMapA(SubSeq(log[l], 1, commit[l]), wstart[l]) ELSE mapA[l]

Conflict(l, t, s) ==
  \E d \in Deps[t] :
    \/ (d.mode = "esiste" /\ VMapS(l)[d.k] > s)
    \/ (d.mode = "valore" /\ VMapA(l)[d.k] > s)

\* vincolo del modello (non del protocollo): il nodo d'ingresso ritenta un id solo finche'
\* la sua eventuale voce e' ancora nella finestra del leader
RetryWithinWindow(l, t) == \A i \in 1..Len(log[l]) : log[l][i].tx = t => i >= wstart[l]

\* Un tentativo della transazione t preparato a S = s. seen = insieme delle posizioni in cui la lettura
\* della chiave di sessione a S ha trovato t (vuoto = non trovato o SessionLookup spento).
SubmitAt(l, t, s, seen) ==
  /\ alive[l] /\ role[l] = "leader" /\ LeaderReady(l)
  /\ IF seen /= {}
     THEN \* la chiave di sessione letta a S porta gia' l'esito: nessun invio
          /\ ~(seen \subseteq outcome[t])
          /\ outcome' = [outcome EXCEPT ![t] = @ \cup seen]
          /\ UNCHANGED <<log, match, mapS, mapA, ids, attempts>>
     ELSE IF ids[l][t] /= 0
     THEN \* id gia' visto dal leader: stesso esito, nessuna voce nuova
          /\ ids[l][t] <= commit[l] /\ ids[l][t] \notin outcome[t]
          /\ outcome' = [outcome EXCEPT ![t] = @ \cup {ids[l][t]}]
          /\ UNCHANGED <<log, match, mapS, mapA, ids, attempts>>
     ELSE \* tentativo nuovo: validazione e voce. Un rifiuto non cambia lo stato (il nodo d'ingresso
          \* ritenta con un'altra S: e' un'altra scelta di s), quindi non e' un passo del modello.
          /\ attempts[t] < MaxAttempts
          /\ RetryBeyondWindow \/ RetryWithinWindow(l, t)
          /\ s >= wstart[l] \/ AcceptOldS                 \* S sotto la finestra: rifiuto
          /\ s <= Len(log[l])                              \* S sconosciuta (leader deposto): rifiuto
          /\ NoValidate \/ ~Conflict(l, t, s)              \* conflitto: rifiuto
          /\ LET c == Len(log[l]) + 1
                 ws == Writes[t]
             IN /\ log' = [log EXCEPT ![l] = Append(@, Entry(epoch[l], t, s))]
                /\ match' = [match EXCEPT ![l][l] = c]
                /\ mapS' = [mapS EXCEPT ![l] = [k \in Keys |-> IF \E w \in ws : w.k = k /\ w.kind = "strutturale" THEN c ELSE @[k]]]
                /\ mapA' = [mapA EXCEPT ![l] = [k \in Keys |-> IF \E w \in ws : w.k = k THEN c ELSE @[k]]]
                /\ ids' = [ids EXCEPT ![l][t] = c]
                /\ attempts' = [attempts EXCEPT ![t] = @ + 1]
                /\ UNCHANGED outcome
  /\ UNCHANGED <<epoch, role, alive, commit, wstart, cmtLog, losses, diskStart>>

\* In questo modulo S e' una posizione confermata qualsiasi, e la lettura della chiave di sessione e'
\* un oracolo sulla storia confermata. LettureScarico (ReadPrepared) le sostituisce con la lettura vera.
Submit(l, t) ==
  \E s \in 0..MaxCommit :
     SubmitAt(l, t, s, IF SessionLookup THEN {i \in 1..s : cmtLog[i].tx = t} ELSE {})

(* ----------------------------------------------------------- replicazione *)

\* APPEND(posPrecedente = p, n voci) dal leader l al seguace f, con n in 0..1 (n = 0 e' il battito).
\* Il controllo del prefisso passa se f ha a p la stessa voce del leader, cioe' p <= m (prefisso comune).
\* Il seguace tronca solo dal primo conflitto (Raft): se le voci ricevute combaciano tutte con le
\* proprie, la propria coda resta, anche se viene da un leader deposto. Per questo la confermata del
\* seguace e' min(confermata del leader, p + n) e NON min(confermata, ultima posizione propria).
\* Senza OldPrev il leader manda sempre p = m (il modello del primo giro).
\* Le voci mandate devono essere ancora sul disco del leader (p + 1 >= diskStart[l]); se il seguace e'
\* sotto, serve la ricarica da S3 (LettureScarico, Reload).
Replicate(l, f, p, n) ==
  /\ alive[l] /\ alive[f] /\ l /= f /\ role[l] = "leader" /\ epoch[f] <= epoch[l]
  /\ LET m == MatchLen(log[l], log[f])
         newLog == IF p + n > m THEN SubSeq(log[l], 1, p + n) ELSE log[f]
         c == Min(commit[l], IF CommitByTail THEN Len(newLog) ELSE p + n)
     IN /\ p <= m /\ p + n <= Len(log[l])
        /\ OldPrev \/ p = m
        /\ n = 0 \/ p + 1 >= diskStart[l]
        /\ newLog /= log[f] \/ c > commit[f]
        /\ log' = [log EXCEPT ![f] = newLog]
        /\ match' = [match EXCEPT ![l][f] = Max({@, p + n})]
        /\ commit' = [commit EXCEPT ![f] = Max({commit[f], c})]
  /\ epoch' = [epoch EXCEPT ![f] = epoch[l]]
  /\ role' = [role EXCEPT ![f] = "follower"]
  /\ UNCHANGED <<alive, wstart, mapS, mapA, ids, attempts, outcome, cmtLog, losses, diskStart>>

AdvanceCommit(l) ==
  /\ alive[l] /\ role[l] = "leader"
  /\ LET agreeing(i) == Cardinality({r \in Replicas : match[l][r] >= i}) >= Majority
         cands == {i \in (commit[l] + 1)..Len(log[l]) : agreeing(i) /\ (CommitOldEpoch \/ log[l][i].ep = epoch[l])}
     IN /\ cands /= {}
        /\ LET c == Max(cands)
           IN /\ commit' = [commit EXCEPT ![l] = c]
              /\ cmtLog' = IF c > Len(cmtLog) THEN SubSeq(log[l], 1, c) ELSE cmtLog
              /\ outcome' = [t \in Txns |-> outcome[t] \cup {i \in (commit[l] + 1)..c : log[l][i].tx = t}]
  /\ UNCHANGED <<epoch, role, alive, log, match, wstart, mapS, mapA, ids, attempts, losses, diskStart>>

(* ------------------------------------------------------- finestra e guasti *)

\* la finestra avanza di una posizione (solo su voci confermate: una voce vecchia di 60 s e' confermata).
\* Il leader dimentica chiavi e id usciti dalla finestra. Con FollowerWindow = FALSE la finestra dei
\* seguaci resta ferma (modello del primo giro, piu' piccolo); con TRUE avanza anche quella: un nuovo
\* leader ricostruisce allora solo dalla propria finestra e rifiuta le S sotto.
AdvanceWindow(s) ==
  /\ alive[s] /\ (role[s] = "leader" \/ FollowerWindow) /\ wstart[s] <= commit[s]
  /\ LET nw == wstart[s] + 1
     IN /\ wstart' = [wstart EXCEPT ![s] = nw]
        /\ IF role[s] = "leader"
           THEN /\ mapS' = [mapS EXCEPT ![s] = [k \in Keys |-> IF @[k] < nw THEN 0 ELSE @[k]]]
                /\ mapA' = [mapA EXCEPT ![s] = [k \in Keys |-> IF @[k] < nw THEN 0 ELSE @[k]]]
                /\ ids' = [ids EXCEPT ![s] = [t \in Txns |-> IF @[t] < nw THEN 0 ELSE @[t]]]
           ELSE UNCHANGED <<mapS, mapA, ids>>
  /\ UNCHANGED <<epoch, role, alive, log, commit, match, attempts, outcome, cmtLog, losses, diskStart>>

Lose(r) ==
  /\ alive[r] /\ losses < MaxLoss
  /\ alive' = [alive EXCEPT ![r] = FALSE]
  /\ losses' = losses + 1
  /\ UNCHANGED <<epoch, role, log, commit, match, wstart, mapS, mapA, ids, attempts, outcome, cmtLog, diskStart>>

Next ==
  \/ \E c \in Replicas, Q \in SUBSET Replicas : Elect(c, Q)
  \/ \E l \in Replicas, t \in Txns : Submit(l, t)
  \/ \E l, f \in Replicas, p \in 0..MaxLog, n \in 0..1 : Replicate(l, f, p, n)
  \/ \E l \in Replicas : AdvanceCommit(l) \/ AdvanceWindow(l) \/ Lose(l)

Spec == Init /\ [][Next]_vars

(* ------------------------------------------------------------- invarianti *)

TypeOK ==
  /\ \A r \in Replicas : epoch[r] \in 0..MaxEpoch /\ role[r] \in {"leader", "follower"}
                        /\ commit[r] <= Len(log[r]) /\ wstart[r] \in 1..(Len(log[r]) + 1)
                        /\ diskStart[r] <= wstart[r]
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
