------------------------------- MODULE Journal3Z -------------------------------
(***************************************************************************)
(* Core of the journal3z protocol (docs/journal3z-design.md section 5) for *)
(* ONE database: 3 journal nodes, writers identified by their epoch (the  *)
(* lease seq), an object store (S3) with a writer fence and a marker      *)
(* ("applied"), upload + trim.                                            *)
(*                                                                         *)
(* Messages live in a set and are never removed: every message can be     *)
(* delivered late, many times, in any order, or never (loss).             *)
(* A node's state is its durable state: a node replies only after         *)
(* fdatasync, so processing a message and persisting are one step.        *)
(* Entries are identified by (index, epoch of the writer that created     *)
(* them): a writer writes one value per index. A log slot is either an    *)
(* entry [e |-> epoch, b |-> barrier?] or "T" (trimmed: in S3).           *)
(*                                                                         *)
(* Negative controls (constants): NoFence (nodes ignore promises),        *)
(* AckOnOne (commit on 1 node), Longest (recovery picks the longest log, *)
(* ignoring epochs), TrimEarly (trim to the writer's log end without an   *)
(* upload). With all FALSE every invariant must hold; with one TRUE, TLC  *)
(* must find a counterexample.                                            *)
(***************************************************************************)
EXTENDS Naturals, Sequences, FiniteSets, TLC

CONSTANTS Nodes, MaxEpoch, MaxIdx, NoFence, AckOnOne, Longest, TrimEarly

Epochs == 1..MaxEpoch
Quorums == {Q \in SUBSET Nodes : Cardinality(Q) * 2 > Cardinality(Nodes)}
None == 0

VARIABLES
  promised,   \* node -> highest epoch promised
  log,        \* node -> sequence of slots
  trim,       \* node -> trim point
  applied,    \* S3 marker
  s3,         \* S3 content: sequence of entries, length = applied
  storeEpoch, \* object-store writer fence
  phase,      \* epoch -> "idle" | "fencing" | "active"
  wapplied,   \* epoch -> marker read at open
  promises,   \* epoch -> set of [n, last, le, suffix]
  wlog,       \* epoch -> the writer's log (entries above wapplied, index = wapplied + position)
  barrier,    \* epoch -> barrier index
  matchIdx,   \* epoch -> node -> highest index acknowledged
  commit,     \* epoch -> commit index
  acked,      \* set of [i, e]: client writes acknowledged
  msgs

vars == <<promised, log, trim, applied, s3, storeEpoch, phase, wapplied, promises, wlog, barrier, matchIdx, commit, acked, msgs>>

Entry(e, b) == [e |-> e, b |-> b]
LastEpoch(l) == IF Len(l) = 0 THEN 0 ELSE IF l[Len(l)] = "T" THEN 0 ELSE l[Len(l)].e
Max(a, b) == IF a > b THEN a ELSE b
\* entry of writer e at absolute index i (or "none")
WEntry(e, i) == IF i > wapplied[e] /\ i - wapplied[e] <= Len(wlog[e]) THEN wlog[e][i - wapplied[e]] ELSE "none"
WLast(e) == wapplied[e] + Len(wlog[e])

Init ==
  /\ promised = [n \in Nodes |-> 0]
  /\ log = [n \in Nodes |-> << >>]
  /\ trim = [n \in Nodes |-> 0]
  /\ applied = 0
  /\ s3 = << >>
  /\ storeEpoch = 0
  /\ phase = [e \in Epochs |-> "idle"]
  /\ wapplied = [e \in Epochs |-> 0]
  /\ promises = [e \in Epochs |-> {}]
  /\ wlog = [e \in Epochs |-> << >>]
  /\ barrier = [e \in Epochs |-> 0]
  /\ matchIdx = [e \in Epochs |-> [n \in Nodes |-> 0]]
  /\ commit = [e \in Epochs |-> 0]
  /\ acked = {}
  /\ msgs = {}

\* trim a node's log to k: slots 1..k become "T" (extend if shorter: S3 may be ahead)
TrimTo(l, k) == [i \in 1..Max(Len(l), k) |-> IF i <= k THEN "T" ELSE l[i]]

(* ---------------------------------------------------------------- writer *)

\* Open: the store fence + marker read (atomic in S3), then FENCE to every node.
Open(e) ==
  /\ phase[e] = "idle"
  /\ storeEpoch < e
  /\ storeEpoch' = e
  /\ wapplied' = [wapplied EXCEPT ![e] = applied]
  /\ phase' = [phase EXCEPT ![e] = "fencing"]
  /\ msgs' = msgs \cup {[t |-> "fence", e |-> e, applied |-> applied]}
  /\ UNCHANGED <<promised, log, trim, applied, s3, promises, wlog, barrier, matchIdx, commit, acked>>

\* Node n handles FENCE: promise and return its log above the marker (equivalent to the later READ:
\* after the promise the node's log can only change by a higher epoch, which makes the read fail).
RecvFence(n) ==
  \E m \in msgs :
    /\ m.t = "fence"
    /\ m.e >= promised[n]
    /\ LET l == IF m.applied > trim[n] THEN TrimTo(log[n], m.applied) ELSE log[n]
           tr == Max(trim[n], m.applied)
       IN /\ log' = [log EXCEPT ![n] = l]
          /\ trim' = [trim EXCEPT ![n] = tr]
          /\ promised' = [promised EXCEPT ![n] = m.e]
          /\ msgs' = msgs \cup {[t |-> "promise", e |-> m.e, n |-> n, last |-> Len(l), le |-> LastEpoch(l),
                                 suffix |-> IF Len(l) > m.applied THEN SubSeq(l, m.applied + 1, Len(l)) ELSE << >>]}
    /\ UNCHANGED <<applied, s3, storeEpoch, phase, wapplied, promises, wlog, barrier, matchIdx, commit, acked>>

RecvPromise(e) ==
  \E m \in msgs :
    /\ m.t = "promise" /\ m.e = e /\ phase[e] = "fencing"
    /\ promises' = [promises EXCEPT ![e] = @ \cup {[n |-> m.n, last |-> m.last, le |-> m.le, suffix |-> m.suffix]}]
    /\ UNCHANGED <<promised, log, trim, applied, s3, storeEpoch, phase, wapplied, wlog, barrier, matchIdx, commit, acked, msgs>>

\* Rule R3: among a quorum of promises, the log with the largest (last epoch, last index) above the marker.
Better(p, q) == IF Longest THEN p.last >= q.last ELSE (p.le > q.le \/ (p.le = q.le /\ p.last >= q.last))
Choose(e) ==
  /\ phase[e] = "fencing"
  /\ \E Q \in Quorums :
       /\ \A n \in Q : \E p \in promises[e] : p.n = n
       /\ LET P == {p \in promises[e] : p.n \in Q}
              cands == {p \in P : p.last > wapplied[e]}
          IN \E best \in P :
               /\ (cands = {} \/ (best \in cands /\ \A q \in cands : Better(best, q)))
               /\ LET rec == IF cands = {} THEN << >> ELSE best.suffix
                  IN /\ wlog' = [wlog EXCEPT ![e] = rec \o << Entry(e, TRUE) >>]
                     /\ barrier' = [barrier EXCEPT ![e] = wapplied[e] + Len(rec) + 1]
  /\ phase' = [phase EXCEPT ![e] = "active"]
  /\ UNCHANGED <<promised, log, trim, applied, s3, storeEpoch, wapplied, promises, matchIdx, commit, acked, msgs>>

ClientWrite(e) ==
  /\ phase[e] = "active"
  /\ WLast(e) < MaxIdx
  /\ wlog' = [wlog EXCEPT ![e] = Append(@, Entry(e, FALSE))]
  /\ UNCHANGED <<promised, log, trim, applied, s3, storeEpoch, phase, wapplied, promises, barrier, matchIdx, commit, acked, msgs>>

\* APPEND from any prev k (models first send, catch-up after MISMATCH, retransmission)
SendAppend(e) ==
  /\ phase[e] = "active"
  /\ \E k \in wapplied[e]..(WLast(e) - 1) :
       msgs' = msgs \cup {[t |-> "append", e |-> e, applied |-> wapplied[e], prev |-> k,
                           pe |-> IF k <= wapplied[e] THEN 0 ELSE WEntry(e, k).e,
                           ents |-> SubSeq(wlog[e], k - wapplied[e] + 1, Len(wlog[e]))]}
  /\ UNCHANGED <<promised, log, trim, applied, s3, storeEpoch, phase, wapplied, promises, wlog, barrier, matchIdx, commit, acked>>

\* Node n handles APPEND (fence, trim hint, consistency check, truncate diverged suffix, append, ack)
RecvAppend(n) ==
  \E m \in msgs :
    /\ m.t = "append"
    /\ (m.e >= promised[n] \/ NoFence)
    /\ LET l0 == IF m.applied > trim[n] THEN TrimTo(log[n], m.applied) ELSE log[n]
           tr == Max(trim[n], m.applied)
           ok == m.prev <= tr \/ (m.prev <= Len(l0) /\ l0[m.prev] /= "T" /\ l0[m.prev].e = m.pe)
           \* Raft: an entry that differs from the writer's (same index, other epoch) is cut with everything after it
           conflict == \E j \in 1..Len(m.ents) : m.prev + j <= Len(l0) /\ l0[m.prev + j] /= "T" /\ l0[m.prev + j] /= m.ents[j]
           top == m.prev + Len(m.ents)
           newl == [i \in 1..(IF conflict THEN Max(top, tr) ELSE Max(Len(l0), top)) |->
                      IF i <= m.prev THEN l0[i]
                      ELSE IF i <= top THEN (IF i <= Len(l0) /\ l0[i] = "T" THEN "T" ELSE m.ents[i - m.prev])
                      ELSE l0[i]]
       IN /\ ok
          /\ log' = [log EXCEPT ![n] = newl]
          /\ trim' = [trim EXCEPT ![n] = tr]
          /\ promised' = [promised EXCEPT ![n] = Max(promised[n], m.e)]
          /\ msgs' = msgs \cup {[t |-> "ack", e |-> m.e, n |-> n, matched |-> m.prev + Len(m.ents)]}
    /\ UNCHANGED <<applied, s3, storeEpoch, phase, wapplied, promises, wlog, barrier, matchIdx, commit, acked>>

RecvAck(e) ==
  \E m \in msgs :
    /\ m.t = "ack" /\ m.e = e /\ phase[e] = "active"
    /\ m.matched > matchIdx[e][m.n]
    /\ LET mi == [matchIdx[e] EXCEPT ![m.n] = m.matched]
           QS == IF AckOnOne THEN {{x} : x \in Nodes} ELSE Quorums
           okc(d) == \E Q \in QS : \A x \in Q : mi[x] >= d
           c == CHOOSE c \in 0..(MaxIdx + 1) : okc(c) /\ \A d \in 0..(MaxIdx + 1) : okc(d) => d <= c
           nc == IF c >= barrier[e] /\ c > commit[e] THEN c ELSE commit[e]
       IN /\ matchIdx' = [matchIdx EXCEPT ![e] = mi]
          /\ commit' = [commit EXCEPT ![e] = nc]
          /\ acked' = acked \cup {[i |-> i, e |-> e] : i \in {j \in (wapplied[e] + 1)..nc : WEntry(e, j) /= "none" /\ WEntry(e, j).e = e /\ ~WEntry(e, j).b}}
    /\ UNCHANGED <<promised, log, trim, applied, s3, storeEpoch, phase, wapplied, promises, wlog, barrier, msgs>>

\* Upload: only the store's current writer; the uploaded state is its log up to some k (acknowledged or not)
Upload(e) ==
  /\ phase[e] = "active" /\ storeEpoch = e
  /\ \E k \in (applied + 1)..WLast(e) :
       /\ s3' = s3 \o [i \in 1..(k - applied) |-> WEntry(e, applied + i)]
       /\ applied' = k
       /\ msgs' = msgs \cup {[t |-> "trim", k |-> k]}
  /\ UNCHANGED <<promised, log, trim, storeEpoch, phase, wapplied, promises, wlog, barrier, matchIdx, commit, acked>>

\* Negative control: trim before (without) the upload
EarlyTrim(e) ==
  /\ TrimEarly /\ phase[e] = "active"
  /\ msgs' = msgs \cup {[t |-> "trim", k |-> WLast(e)]}
  /\ UNCHANGED <<promised, log, trim, applied, s3, storeEpoch, phase, wapplied, promises, wlog, barrier, matchIdx, commit, acked>>

RecvTrim(n) ==
  \E m \in msgs :
    /\ m.t = "trim" /\ m.k > trim[n]
    /\ log' = [log EXCEPT ![n] = TrimTo(@, m.k)]
    /\ trim' = [trim EXCEPT ![n] = m.k]
    /\ UNCHANGED <<promised, applied, s3, storeEpoch, phase, wapplied, promises, wlog, barrier, matchIdx, commit, acked, msgs>>

Next ==
  \/ \E e \in Epochs : Open(e) \/ RecvPromise(e) \/ Choose(e) \/ ClientWrite(e) \/ SendAppend(e) \/ RecvAck(e) \/ Upload(e) \/ EarlyTrim(e)
  \/ \E n \in Nodes : RecvFence(n) \/ RecvAppend(n) \/ RecvTrim(n)

Spec == Init /\ [][Next]_vars

(* ------------------------------------------------------------ invariants *)

\* the log recovery would choose from quorum Q (rule R3), as absolute slots above the marker
Chosen(Q) ==
  LET cands == {n \in Q : Len(log[n]) > applied}
      better(a, b) == LastEpoch(log[a]) > LastEpoch(log[b]) \/ (LastEpoch(log[a]) = LastEpoch(log[b]) /\ Len(log[a]) >= Len(log[b]))
  IN IF cands = {} THEN "none" ELSE CHOOSE a \in cands : \A b \in cands : better(a, b)

\* (a) every acknowledged write is in S3 below the marker, or in the log recovery chooses from EVERY quorum
Durability ==
  \A a \in acked :
    \/ (a.i <= applied /\ s3[a.i] = Entry(a.e, FALSE))
    \/ \A Q \in Quorums :
         LET c == Chosen(Q) IN c /= "none" /\ a.i <= Len(log[c]) /\ log[c][a.i] = Entry(a.e, FALSE)

\* (b2) one index, one acknowledged write
NoTwoAcks == \A a, b \in acked : a.i = b.i => a.e = b.e

\* (b3) acknowledgements of an older epoch are all before the barrier of a newer epoch that committed it
OrderedEpochs == \A a \in acked : \A e \in Epochs : (e > a.e /\ commit[e] >= barrier[e] /\ barrier[e] > 0) => a.i < barrier[e]

\* (c1) log matching: same index and epoch on two nodes -> same entry (b flag) and same earlier entries above both trims
LogMatching ==
  \A n, m \in Nodes : \A i \in 1..(MaxIdx + 1) :
    (i <= Len(log[n]) /\ i <= Len(log[m]) /\ log[n][i] /= "T" /\ log[m][i] /= "T" /\ log[n][i].e = log[m][i].e)
      => \A j \in 1..i : (log[n][j] /= "T" /\ log[m][j] /= "T") => log[n][j] = log[m][j]

\* (d) trim never passes the S3 marker; S3 agrees with every acknowledged write below the marker
TrimSafe == \A n \in Nodes : trim[n] <= applied
S3Agrees == \A a \in acked : a.i <= applied => s3[a.i] = Entry(a.e, FALSE)

TypeOK == applied <= MaxIdx + 1 /\ \A n \in Nodes : Len(log[n]) <= MaxIdx + 1

=============================================================================
