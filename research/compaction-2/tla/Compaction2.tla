----------------------------- MODULE Compaction2 -----------------------------
(***************************************************************************)
(* Compaction 2, step 0: the paper check of way (b).                       *)
(*                                                                         *)
(* SlateDB keeps the write path (memtable, L0 SSTs, one sorted run that    *)
(* every drop job folds into). Our side keeps a manifest (versions in the  *)
(* store, written with a conditional create), delta runs over the whole    *)
(* keyspace and one sealed run per group. Every row keeps the SlateDB      *)
(* sequence number it was written with. A version holds X_d (every         *)
(* SlateDB row with seq <= X_d is in our runs) and, per group, X_g <= X_d  *)
(* (every row of the group with seq <= X_g is in its sealed run).          *)
(*                                                                         *)
(* Modelled, from the plan and both review rounds:                         *)
(*  - writes (put, delete envelope, customer erasure as a range tombstone),*)
(*    flushes, absorption of the oldest L0 SST into a new delta run,       *)
(*    delta merges, group reseals, one group merge rewritten at X_d;       *)
(*  - publishes in two steps (the PUT starts, then lands or is lost), so a *)
(*    publish can be slow, and can still land after the compactor crashed; *)
(*  - drop jobs that fold the absorbed L0 SSTs and the sorted run into the *)
(*    sorted run, with the floor computed inside the job from versions     *)
(*    seen in the store; the lease counts from the first sight;            *)
(*  - drop jobs in the hold owner's process (floor also below every live   *)
(*    hold) and in a remote pool (time-only floor, lease > hold life);     *)
(*  - holds of a find reader and of a transaction in the owner's process,  *)
(*    revoked at the hold life, checked after every step;                  *)
(*  - a follower in another process that refreshes the version it knows    *)
(*    and checks the T_pin age before every step that opens a SlateDB view;*)
(*  - one restore point taken by the control layer under a lease in the    *)
(*    store, paired with a version; export and PITR read through the pair; *)
(*  - one fork / PITR clone from that checkpoint, with its own E_env =     *)
(*    min(parent E_env, checkpoint last_l0_seq), sharing runs by reference;*)
(*  - release E0 -> E1 (envelope writing starts at seq E_env);             *)
(*  - GC of retired runs after a grace longer than the hold life, keeping  *)
(*    runs listed by the checkpoint pair, its lease and the fork;          *)
(*  - one crash of the owner's process (holds, first sights and the        *)
(*    compactor's memory are lost; the WAL keeps the memtable; a PUT in    *)
(*    flight may still land).                                              *)
(*                                                                         *)
(* Time is discrete: Tick ages every clock by one, saturating at Cap, so   *)
(* the model is finite. Constants: Tpin < Lease < HoldLife < RemoteLease,  *)
(* HoldLife < Grace.                                                       *)
(*                                                                         *)
(* Negative controls: the constant Ctl names one broken variant ("none" is *)
(* the correct model). See README.md in this folder.                       *)
(***************************************************************************)
EXTENDS Integers, FiniteSets, Sequences, TLC

CONSTANTS Keys, Groups, Cust, Grp0,      \* keys, groups, key -> customer, key -> first group
          MaxSeq,                        \* parent writes
          MaxComp,                       \* rewrites: delta merges + reseals + group merges
          MaxRuns, MaxVer, MaxCrash, MaxFork,
          Tpin, Lease, HoldLife, RemoteLease, Grace, Cap,
          Readers,                       \* holds in the owner's process: "find", "txn"
          UseFollower, UseCkpt,          \* model the follower / the checkpoint (TRUE in main)
          E0Start,                       \* TRUE: start in E0 or E1; FALSE: start in E1
          Lean,                          \* TRUE: no lost PUTs, remote drops or GC (deep controls only)
          Ctl                            \* "none" or the name of one negative control

Controls == {"none", "DropBeforePublish", "GcNoGrace", "ReadNoCut", "ResealSkip",
             "HoldsIgnored", "TxnRereads", "LeaseFromStamp", "FollowerCheckOnce",
             "RevokeNoCheck", "DropRemoteShortLease", "ForkCopyEenv", "CkptNoLease",
             "MergeConcat", "DeltaDropEnv", "FollowerNoTpin", "ForkNewestCut",
             "MergeAtMaxCut"}
ASSUME Ctl \in Controls
ASSUME Readers \subseteq {"find", "txn"}
ASSUME Tpin < Lease /\ Lease < HoldLife /\ HoldLife < RemoteLease /\ HoldLife < Grace
ASSUME RemoteLease <= Cap /\ Grace <= Cap

Customers == {Cust[k] : k \in Keys}
Inf     == MaxSeq + 100          \* E_env before release E1
RunIds  == 1..MaxRuns
VerIds  == 1..MaxVer
Min(a, b) == IF a < b THEN a ELSE b
Max(a, b) == IF a > b THEN a ELSE b
MinSet(S) == CHOOSE x \in S : \A y \in S : x <= y
MaxSet(S) == CHOOSE x \in S : \A y \in S : x >= y
Gmerge  == CHOOSE g \in Groups : TRUE                \* the surviving group of a merge
KeysOfCust(c) == {k \in Keys : Cust[k] = c}

(* A row: t = "put" | "del" (envelope) | "erase" (range tombstone over a   *)
(* customer's whole key range, k = the customer); s = its sequence number; *)
(* enc = written in envelope format. A put's value is its s.               *)
Row(t, k, s, enc) == [t |-> t, k |-> k, s |-> s, enc |-> enc]
RKey(r) == <<r.t = "erase", r.k>>
NewestPerKey(R) == {r \in R : \A q \in R : RKey(q) = RKey(r) => q.s <= r.s}
Relevant(r, k) == (r.t # "erase" /\ r.k = k) \/ (r.t = "erase" /\ r.k = Cust[k])
\* the value of key k from a set of candidate rows: the newest relevant row wins
ValOf(R, k) ==
  LET rel == {r \in R : Relevant(r, k)}
  IN IF rel = {} THEN 0
     ELSE LET w == CHOOSE r \in rel : \A q \in rel : q.s <= r.s
          IN IF w.t = "put" THEN w.s ELSE 0
SeqsOf(R) == {r.s : r \in R}

VARIABLES
  seq,      \* last acknowledged sequence number (parent)
  hist,     \* seq -> the write [t, k]
  tr,       \* ghost: seq -> (key -> value) after that write; tr[0] all absent
  eenv,     \* E_env of the parent
  mem,      \* memtable rows (durable through the WAL)
  l0,       \* L0 SSTs, oldest first: sequence of row sets
  sr,       \* SlateDB's single sorted run (rows)
  flushed,  \* last_l0_seq
  ver,      \* our manifest versions in the store (sequence of records)
  seen,     \* version -> age since a floor computation first saw it, -1 = never
  stamp,    \* version -> age since the stamp written inside it (only LeaseFromStamp)
  runSt,    \* run -> "free" | "live" | "gone"
  runRows,  \* run -> rows
  rage,     \* run -> age since it left the newest version (or was orphaned), -1 = in use
  job,      \* the publish in flight: [k |-> "none"] or [k, base, rec, runs, st]
  nComp, nMerge,
  rd,       \* reader -> [ph |-> "idle"|"held"|"revoked", v, S, age]
  fl,       \* follower: [v, age, ph |-> "idle"|"op", steps]
  ck,       \* checkpoint: [st |-> "none"|"begun"|"taken", v, rows, cs, lease]
  fk,       \* fork / PITR clone
  crashes,
  bad       \* failures seen by readers: set of strings

vars == <<seq, hist, tr, eenv, mem, l0, sr, flushed, ver, seen, stamp, runSt, runRows,
          rage, job, nComp, nMerge, rd, fl, ck, fk, crashes, bad>>

NoJob  == [k |-> "none"]
NoFork == [on |-> FALSE]
Latest == ver[Len(ver)]
NV     == Len(ver)
RunsOf(V) == V.delta \cup ({V.sealed[g] : g \in Groups} \ {0})
RowsOfRuns(S) == UNION {runRows[r] : r \in S}
DeltaRows(V) == RowsOfRuns(V.delta)
SealedRows(V, g) == IF V.sealed[g] = 0 THEN {} ELSE runRows[V.sealed[g]]
L0Rows == UNION {l0[i] : i \in 1..Len(l0)}
SlateRows == mem \cup L0Rows \cup sr            \* what the writer's SlateDB view holds
FlushedRows == L0Rows \cup sr                   \* what a follower (DbReader) sees
\* the group of a row in version V
GroupOf(V, r) == IF r.t = "erase" THEN V.own[CHOOSE k \in KeysOfCust(r.k) : TRUE]
                 ELSE V.own[r.k]

-----------------------------------------------------------------------------
(* The read rule at position S with version V and SlateDB rows R:          *)
(*   SlateDB rows with X_d < seq <= S, delta rows of k's group with         *)
(*   X_g < seq <= X_d, the sealed run of k's group. ReadNoCut drops the     *)
(*   bounds (the newest row of any tier wins).                              *)
Cand(V, R, S, k) ==
  LET g == V.own[k] IN
  IF Ctl = "ReadNoCut"
  THEN {r \in R : r.s <= S} \cup DeltaRows(V) \cup SealedRows(V, g)
  ELSE {r \in R : V.xd < r.s /\ r.s <= S}
       \cup {r \in DeltaRows(V) : GroupOf(V, r) = g /\ V.xg[g] < r.s /\ r.s <= V.xd}
       \cup SealedRows(V, g)
Read(V, R, S, k) == ValOf(Cand(V, R, S, k), k)
RunsLive(V) == \A r \in RunsOf(V) : runSt[r] = "live"

-----------------------------------------------------------------------------
(* Rewrites. Reseal and group merge apply erasures and drop every envelope *)
(* (all their output is at or below the new X_g).                          *)
ApplyErase(R) ==
  {r \in R : r.t = "put" /\ ~\E e \in R : e.t = "erase" /\ e.k = Cust[r.k] /\ e.s > r.s}
SealOut(R) == ApplyErase(NewestPerKey(R))
\* delta rows of group g with lo < seq <= hi, from the runs DS
DeltaIn(V, DS, g, lo, hi) ==
  {r \in RowsOfRuns(DS) : GroupOf(V, r) = g /\ lo < r.s /\ r.s <= hi}

Init ==
  /\ seq = 0
  /\ hist = [s \in 1..MaxSeq |-> [t |-> "none", k |-> "none"]]
  /\ tr = [s \in 0..MaxSeq |-> [k \in Keys |-> 0]]
  /\ eenv \in IF E0Start THEN {0, Inf} ELSE {0}   \* E1 from the start, or E0 then E1
  /\ mem = {} /\ l0 = <<>> /\ sr = {} /\ flushed = 0
  /\ ver = << [xd |-> 0, xg |-> [g \in Groups |-> 0], delta |-> {},
               sealed |-> [g \in Groups |-> 0], own |-> Grp0] >>
  /\ seen = [v \in VerIds |-> -1]
  /\ stamp = [v \in VerIds |-> -1]
  /\ runSt = [r \in RunIds |-> "free"]
  /\ runRows = [r \in RunIds |-> {}]
  /\ rage = [r \in RunIds |-> -1]
  /\ job = NoJob /\ nComp = 0 /\ nMerge = 0
  /\ rd = [x \in Readers |-> [ph |-> "idle", v |-> 0, S |-> 0, age |-> 0]]
  /\ fl = [v |-> 1, age |-> 0, ph |-> "idle", steps |-> 0]
  /\ ck = [st |-> "none", v |-> 0, rows |-> {}, cs |-> 0, lease |-> FALSE]
  /\ fk = NoFork
  /\ crashes = 0
  /\ bad = {}

-----------------------------------------------------------------------------
(* The write path (SlateDB, unchanged by way (b)).                         *)

Write(t, k) ==
  /\ seq < MaxSeq
  /\ t # "put" => eenv # Inf                 \* E0 writes no envelopes
  /\ LET s == seq + 1
         enc == s > eenv
         kk == IF t = "erase" THEN Cust[k] ELSE k
         nv == [x \in Keys |-> IF (t = "erase" /\ Cust[x] = Cust[k]) \/ (t # "erase" /\ x = k)
                                THEN (IF t = "put" THEN s ELSE 0) ELSE tr[seq][x]]
     IN /\ seq' = s
        /\ hist' = [hist EXCEPT ![s] = [t |-> t, k |-> kk]]
        /\ tr' = [tr EXCEPT ![s] = nv]
        /\ mem' = mem \cup {Row(t, kk, s, enc)}
  /\ UNCHANGED <<eenv, l0, sr, flushed, ver, seen, stamp, runSt, runRows, rage, job,
                 nComp, nMerge, rd, fl, ck, fk, crashes, bad>>

EnableE1 ==                                   \* release E1 starts envelope writing
  /\ eenv = Inf
  /\ eenv' = seq
  /\ UNCHANGED <<seq, hist, tr, mem, l0, sr, flushed, ver, seen, stamp, runSt, runRows,
                 rage, job, nComp, nMerge, rd, fl, ck, fk, crashes, bad>>

Flush ==
  /\ mem # {}
  /\ l0' = Append(l0, mem)
  /\ mem' = {}
  /\ flushed' = seq
  /\ UNCHANGED <<seq, hist, tr, eenv, sr, ver, seen, stamp, runSt, runRows, rage, job,
                 nComp, nMerge, rd, fl, ck, fk, crashes, bad>>

-----------------------------------------------------------------------------
(* The compactor (owner's process): one publish in flight at a time.       *)

FreeRun == CHOOSE r \in RunIds : runSt[r] = "free" /\ \A q \in RunIds : runSt[q] = "free" => r <= q
HasFree == \E r \in RunIds : runSt[r] = "free"

\* start a publish: write the new run (if any rows), stamp the version, start the PUT
StartJob(kind, rec, rows) ==
  /\ NV < MaxVer
  /\ IF rows = {}
     THEN /\ job' = [k |-> kind, base |-> NV, rec |-> rec, runs |-> {}, st |-> 0]
          /\ UNCHANGED <<runSt, runRows>>
     ELSE /\ HasFree
          /\ LET r == FreeRun
                 rec2 == IF kind \in {"absorb", "dmerge"} THEN [rec EXCEPT !.delta = @ \cup {r}]
                         ELSE [rec EXCEPT !.sealed = [g \in Groups |->
                                 IF rec.sealed[g] = -1 THEN r ELSE rec.sealed[g]]]
             IN /\ runSt' = [runSt EXCEPT ![r] = "live"]
                /\ runRows' = [runRows EXCEPT ![r] = rows]
                /\ job' = [k |-> kind, base |-> NV, rec |-> rec2, runs |-> {r}, st |-> 0]

\* the sealed slot of a new sealed run is written as -1 in rec and filled by StartJob
Absorb ==
  /\ job = NoJob
  /\ \E i \in 1..Len(l0) :
       /\ \A q \in SeqsOf(l0[i]) : q > Latest.xd                \* not absorbed yet
       /\ \A j \in 1..(i-1) : \A q \in SeqsOf(l0[j]) : q <= Latest.xd  \* oldest first (I3)
       /\ StartJob("absorb", [Latest EXCEPT !.xd = MaxSet(SeqsOf(l0[i]))],
                   NewestPerKey(l0[i]))
  /\ UNCHANGED <<seq, hist, tr, eenv, mem, l0, sr, flushed, ver, seen, stamp, rage,
                 nComp, nMerge, rd, fl, ck, fk, crashes, bad>>

\* delta merge of two runs: rows at or below their group's X_g are dead and go;
\* an envelope above X_g stays (I1). DeltaDropEnv drops every envelope.
DMergeOut(V, R) ==
  {r \in NewestPerKey(R) : r.s > V.xg[GroupOf(V, r)]
                           /\ (Ctl = "DeltaDropEnv" => r.t = "put")}
DeltaMerge ==
  /\ job = NoJob /\ nComp < MaxComp
  /\ \E r1, r2 \in Latest.delta :
       /\ r1 < r2
       /\ StartJob("dmerge", [Latest EXCEPT !.delta = @ \ {r1, r2}],
                   DMergeOut(Latest, runRows[r1] \cup runRows[r2]))
  /\ nComp' = nComp + 1
  /\ UNCHANGED <<seq, hist, tr, eenv, mem, l0, sr, flushed, ver, seen, stamp, rage,
                 nMerge, rd, fl, ck, fk, crashes, bad>>

\* reseal group g at X_d: its sealed run plus its delta rows above X_g.
\* ResealSkip leaves out one delta run.
Reseal ==
  /\ job = NoJob /\ nComp < MaxComp
  /\ \E g \in Groups :
       /\ \E k \in Keys : Latest.own[k] = g
       /\ Latest.xg[g] < Latest.xd
       /\ \E DS \in IF Ctl = "ResealSkip" THEN {Latest.delta \ {x} : x \in Latest.delta}
                                          ELSE {Latest.delta} :
            LET out == SealOut(SealedRows(Latest, g) \cup DeltaIn(Latest, DS, g, Latest.xg[g], Latest.xd))
                rec == [Latest EXCEPT !.xg[g] = Latest.xd,
                                      !.sealed[g] = IF out = {} THEN 0 ELSE -1]
            IN StartJob("reseal", rec, out)
  /\ nComp' = nComp + 1
  /\ UNCHANGED <<seq, hist, tr, eenv, mem, l0, sr, flushed, ver, seen, stamp, rage,
                 nMerge, rd, fl, ck, fk, crashes, bad>>

\* merge every other group into Gmerge: a rewrite at a common cut C >= both X_g (I2).
\* The correct model uses C = X_d: the plan's C = max(X_gA, X_gB) (control MergeAtMaxCut)
\* keeps reads right but can copy an erased put into the new sealed run, when a delta
\* merge has already folded the older erasure into a newer one above C.
\* MergeConcat concatenates the sealed runs and takes the smaller X_g.
GroupMerge ==
  /\ job = NoJob /\ nComp < MaxComp /\ nMerge < 1
  /\ \E g2 \in Groups \ {Gmerge} :
       /\ \E k \in Keys : Latest.own[k] = g2
       /\ LET g1 == Gmerge
              x1 == Latest.xg[g1]
              x2 == Latest.xg[g2]
              C  == CASE Ctl = "MergeConcat"   -> Min(x1, x2)
                      [] Ctl = "MergeAtMaxCut" -> Max(x1, x2)
                      [] OTHER                 -> Latest.xd
              out == IF Ctl = "MergeConcat"
                     THEN SealedRows(Latest, g1) \cup SealedRows(Latest, g2)
                     ELSE SealOut(SealedRows(Latest, g1) \cup SealedRows(Latest, g2)
                                  \cup DeltaIn(Latest, Latest.delta, g1, x1, C)
                                  \cup DeltaIn(Latest, Latest.delta, g2, x2, C))
              rec == [Latest EXCEPT !.xg = [g \in Groups |-> IF g \in {g1, g2} THEN C ELSE Latest.xg[g]],
                                    !.sealed = [g \in Groups |->
                                       IF g = g1 THEN (IF out = {} THEN 0 ELSE -1)
                                       ELSE IF g = g2 THEN 0 ELSE Latest.sealed[g]],
                                    !.own = [k \in Keys |-> IF Latest.own[k] = g2 THEN g1 ELSE Latest.own[k]]]
          IN StartJob("gmerge", rec, out)
  /\ nComp' = nComp + 1 /\ nMerge' = nMerge + 1
  /\ UNCHANGED <<seq, hist, tr, eenv, mem, l0, sr, flushed, ver, seen, stamp, rage,
                 rd, fl, ck, fk, crashes, bad>>

\* the PUT lands: a conditional create of version NV+1 (fails if another landed)
PublishLand ==
  /\ job # NoJob
  /\ IF job.base = NV
     THEN /\ ver' = Append(ver, job.rec)
          /\ rage' = [r \in RunIds |-> IF r \in RunsOf(Latest) \ RunsOf(job.rec) THEN 0 ELSE rage[r]]
          /\ stamp' = IF Ctl = "LeaseFromStamp" THEN [stamp EXCEPT ![NV + 1] = job.st] ELSE stamp
     ELSE /\ rage' = [r \in RunIds |-> IF r \in job.runs THEN 0 ELSE rage[r]]
          /\ UNCHANGED <<ver, stamp>>
  /\ job' = NoJob
  /\ UNCHANGED <<seq, hist, tr, eenv, mem, l0, sr, flushed, seen, runSt, runRows,
                 nComp, nMerge, rd, fl, ck, fk, crashes, bad>>

\* the PUT is lost (error, timeout): the new run is an orphan
PublishLost ==
  /\ job # NoJob
  /\ rage' = [r \in RunIds |-> IF r \in job.runs THEN 0 ELSE rage[r]]
  /\ job' = NoJob
  /\ UNCHANGED <<seq, hist, tr, eenv, mem, l0, sr, flushed, ver, seen, stamp, runSt, runRows,
                 nComp, nMerge, rd, fl, ck, fk, crashes, bad>>

-----------------------------------------------------------------------------
(* Drop jobs: fold the absorbed L0 SSTs and the sorted run into the sorted *)
(* run, keeping only rows above the floor. The floor is computed inside    *)
(* the job (the filter), from the versions in the store.                   *)

Absorbed(i) == \A q \in SeqsOf(l0[i]) : q <= Latest.xd
\* newest version whose first sight (or stamp, LeaseFromStamp) is at least L old
UsableX(L) ==
  LET ok == {v \in 1..NV : IF Ctl = "LeaseFromStamp" THEN stamp[v] >= L \/ (v = 1)
                           ELSE seen[v] >= L}
  IN IF ok = {} THEN 0 ELSE ver[MaxSet(ok)].xd
HeldX == {ver[rd[x].v].xd : x \in {y \in Readers : rd[y].ph = "held"}}
CkptBlocks == ck.st = "begun" /\ ck.lease
\* the floor of a drop job in the owner's process / in the remote pool
OwnerFloor ==
  IF Ctl = "DropBeforePublish"
  THEN (IF job # NoJob /\ job.k = "absorb" THEN job.rec.xd ELSE Latest.xd)
  ELSE IF Ctl = "HoldsIgnored" THEN UsableX(Lease)
  ELSE MinSet({UsableX(Lease)} \cup HeldX)
RemoteFloor == UsableX(IF Ctl = "DropRemoteShortLease" THEN Lease ELSE RemoteLease)

Fold(floor) ==
  LET xd == IF Ctl = "DropBeforePublish" THEN floor ELSE Latest.xd
      nA == CHOOSE m \in 0..Len(l0) : (\A i \in 1..m : \A q \in SeqsOf(l0[i]) : q <= xd)
                                      /\ (m = Len(l0) \/ ~\A q \in SeqsOf(l0[m + 1]) : q <= xd)
      src == sr \cup UNION {l0[i] : i \in 1..nA}
  IN <<SubSeq(l0, nA + 1, Len(l0)), {r \in src : r.s > floor}>>

\* every floor computation records first sights of the versions it sees
SeeAll == [v \in VerIds |-> IF v <= NV /\ seen[v] = -1 THEN 0 ELSE seen[v]]

DropJob(floor) ==
  /\ ~CkptBlocks
  /\ LET f == Fold(floor) IN
     /\ <<f[1], f[2], SeeAll>> # <<l0, sr, seen>>
     /\ l0' = f[1] /\ sr' = f[2] /\ seen' = SeeAll
  /\ UNCHANGED <<seq, hist, tr, eenv, mem, flushed, ver, stamp, runSt, runRows, rage, job,
                 nComp, nMerge, rd, fl, ck, fk, crashes, bad>>
OwnerDrop  == DropJob(OwnerFloor)
RemoteDrop == Ctl # "DropBeforePublish" /\ DropJob(RemoteFloor)

-----------------------------------------------------------------------------
(* GC of runs: retired or orphaned for at least Grace, and not listed by    *)
(* the checkpoint pair, its lease or the fork.                              *)
Referenced ==
  (IF ck.st # "none" THEN RunsOf(ver[ck.v]) ELSE {})
  \cup (IF fk.on THEN RunsOf(fk.v) ELSE {})
GC ==
  /\ \E r \in RunIds :
       /\ runSt[r] = "live" /\ rage[r] >= 0
       /\ rage[r] >= (IF Ctl = "GcNoGrace" THEN 0 ELSE Grace)
       /\ r \notin Referenced
       /\ runSt' = [runSt EXCEPT ![r] = "gone"]
       /\ rage' = [rage EXCEPT ![r] = -1]
  /\ UNCHANGED <<seq, hist, tr, eenv, mem, l0, sr, flushed, ver, seen, stamp, runRows,
                 job, nComp, nMerge, rd, fl, ck, fk, crashes, bad>>

-----------------------------------------------------------------------------
(* Holds in the owner's process: a find reader and a transaction. Begin    *)
(* pins the newest version and takes S = the last acknowledged seq, as one *)
(* pair. Every step reads one key, then checks the hold (revocation).      *)
(* TxnRereads: the transaction reads with the newest version at each step. *)
(* RevokeNoCheck: a revoked hold keeps reading.                            *)

RBegin(x) ==
  /\ rd[x].ph = "idle"
  /\ rd' = [rd EXCEPT ![x] = [ph |-> "held", v |-> NV, S |-> seq, age |-> 0]]
  /\ UNCHANGED <<seq, hist, tr, eenv, mem, l0, sr, flushed, ver, seen, stamp, runSt, runRows,
                 rage, job, nComp, nMerge, fl, ck, fk, crashes, bad>>

RStep(x) ==
  /\ rd[x].ph = "held" \/ (rd[x].ph = "revoked" /\ Ctl = "RevokeNoCheck")
  /\ \E k \in Keys :
       LET V == IF x = "txn" /\ Ctl = "TxnRereads" THEN Latest ELSE ver[rd[x].v]
           got == Read(V, SlateRows, rd[x].S, k)
       IN bad' = bad \cup (IF ~RunsLive(V) THEN {x \o ":gone"} ELSE {})
                     \cup (IF got # tr[rd[x].S][k] THEN {x \o ":wrong"} ELSE {})
  /\ UNCHANGED <<seq, hist, tr, eenv, mem, l0, sr, flushed, ver, seen, stamp, runSt, runRows,
                 rage, job, nComp, nMerge, rd, fl, ck, fk, crashes>>

\* the check after a step finds the hold revoked: the operation fails (retryable)
RAbort(x) ==
  /\ rd[x].ph = "revoked"
  /\ rd' = [rd EXCEPT ![x] = [ph |-> "idle", v |-> 0, S |-> 0, age |-> 0]]
  /\ UNCHANGED <<seq, hist, tr, eenv, mem, l0, sr, flushed, ver, seen, stamp, runSt, runRows,
                 rage, job, nComp, nMerge, fl, ck, fk, crashes, bad>>

REnd(x) ==
  /\ rd[x].ph = "held"
  /\ rd' = [rd EXCEPT ![x] = [ph |-> "idle", v |-> 0, S |-> 0, age |-> 0]]
  /\ UNCHANGED <<seq, hist, tr, eenv, mem, l0, sr, flushed, ver, seen, stamp, runSt, runRows,
                 rage, job, nComp, nMerge, fl, ck, fk, crashes, bad>>

-----------------------------------------------------------------------------
(* The follower (another process: a DbReader, the warm spare, prefetch).   *)
(* It refreshes the version it knows from the store; every step opens a    *)
(* fresh SlateDB view (FollowLatest: flushed rows, S = last_l0_seq) and is *)
(* refused if the last refresh is T_pin old or older.                      *)

FRefresh ==
  /\ UseFollower
  /\ fl' = [fl EXCEPT !.v = NV, !.age = 0]
  /\ fl # fl'
  /\ UNCHANGED <<seq, hist, tr, eenv, mem, l0, sr, flushed, ver, seen, stamp, runSt, runRows,
                 rage, job, nComp, nMerge, rd, ck, fk, crashes, bad>>

FFresh == fl.age < Tpin \/ Ctl = "FollowerNoTpin"
FStart ==
  /\ UseFollower /\ fl.ph = "idle" /\ FFresh
  /\ fl' = [fl EXCEPT !.ph = "op", !.steps = 0]
  /\ UNCHANGED <<seq, hist, tr, eenv, mem, l0, sr, flushed, ver, seen, stamp, runSt, runRows,
                 rage, job, nComp, nMerge, rd, ck, fk, crashes, bad>>

FStep ==
  /\ fl.ph = "op" /\ fl.steps < 2
  /\ IF FFresh \/ Ctl = "FollowerCheckOnce"
     THEN /\ \E k \in Keys :
               LET V == ver[fl.v]
                   got == Read(V, FlushedRows, flushed, k)
               IN bad' = bad \cup (IF ~RunsLive(V) THEN {"follower:gone"} ELSE {})
                             \cup (IF got # tr[flushed][k] THEN {"follower:wrong"} ELSE {})
          /\ fl' = [fl EXCEPT !.steps = fl.steps + 1, !.ph = IF fl.steps + 1 = 2 THEN "idle" ELSE "op"]
     ELSE /\ fl' = [fl EXCEPT !.ph = "idle"]             \* refused, retried after a refresh
          /\ UNCHANGED bad
  /\ UNCHANGED <<seq, hist, tr, eenv, mem, l0, sr, flushed, ver, seen, stamp, runSt, runRows,
                 rage, job, nComp, nMerge, rd, ck, fk, crashes>>

-----------------------------------------------------------------------------
(* The restore point, taken by the control layer: begin reads the newest   *)
(* version and takes the drop lease in the store (CkptNoLease: no lease),  *)
(* take copies SlateDB's checkpoint state (flushed rows, last_l0_seq).     *)

CkBegin ==
  /\ UseCkpt /\ ck.st = "none"
  /\ ck' = [st |-> "begun", v |-> NV, rows |-> {}, cs |-> 0, lease |-> Ctl # "CkptNoLease"]
  /\ UNCHANGED <<seq, hist, tr, eenv, mem, l0, sr, flushed, ver, seen, stamp, runSt, runRows,
                 rage, job, nComp, nMerge, rd, fl, fk, crashes, bad>>
CkTake ==
  /\ ck.st = "begun"
  /\ ck' = [ck EXCEPT !.st = "taken", !.rows = FlushedRows, !.cs = flushed, !.lease = FALSE]
  /\ UNCHANGED <<seq, hist, tr, eenv, mem, l0, sr, flushed, ver, seen, stamp, runSt, runRows,
                 rage, job, nComp, nMerge, rd, fl, fk, crashes, bad>>

(* The fork / PITR staging clone from the checkpoint, after release E1.    *)
(* It continues at the checkpoint's seq, shares the paired version's runs, *)
(* and gets E_env = min(parent E_env, last_l0_seq) (ForkCopyEenv: the      *)
(* parent's E_env). ForkNewestCut pairs it with the parent's newest version.*)
Fork ==
  /\ ck.st = "taken" /\ ~fk.on /\ eenv # Inf /\ MaxFork > 0
  /\ fk' = [on |-> TRUE,
            v |-> IF Ctl = "ForkNewestCut" THEN Latest ELSE ver[ck.v],
            rows |-> ck.rows, seq |-> ck.cs,
            eenv |-> IF Ctl = "ForkCopyEenv" THEN eenv ELSE Min(eenv, ck.cs),
            tr |-> tr[ck.cs], wrote |-> FALSE]
  /\ UNCHANGED <<seq, hist, tr, eenv, mem, l0, sr, flushed, ver, seen, stamp, runSt, runRows,
                 rage, job, nComp, nMerge, rd, fl, ck, crashes, bad>>

ForkWrite(t, k) ==
  /\ fk.on /\ ~fk.wrote /\ t \in {"put", "del"}
  /\ LET s == fk.seq + 1 IN
     fk' = [fk EXCEPT !.rows = @ \cup {Row(t, k, s, TRUE)}, !.seq = s, !.wrote = TRUE,
                      !.tr = [@ EXCEPT ![k] = IF t = "put" THEN s ELSE 0]]
  /\ UNCHANGED <<seq, hist, tr, eenv, mem, l0, sr, flushed, ver, seen, stamp, runSt, runRows,
                 rage, job, nComp, nMerge, rd, fl, ck, crashes, bad>>

-----------------------------------------------------------------------------
(* One crash of the owner's process. The WAL keeps the memtable; holds,    *)
(* first sights and the compactor's memory are lost; a PUT in flight may   *)
(* still land or be lost later (PublishLand / PublishLost).                *)
Crash ==
  /\ crashes < MaxCrash
  /\ crashes' = crashes + 1
  /\ rd' = [x \in Readers |-> [ph |-> "idle", v |-> 0, S |-> 0, age |-> 0]]
  /\ seen' = [v \in VerIds |-> -1]
  /\ UNCHANGED <<seq, hist, tr, eenv, mem, l0, sr, flushed, ver, stamp, runSt, runRows,
                 rage, job, nComp, nMerge, fl, ck, fk, bad>>

-----------------------------------------------------------------------------
(* Time. A hold reaching its life is revoked (it leaves the floor).        *)
Up(a) == IF a < 0 THEN a ELSE Min(a + 1, Cap)
Tick ==
  /\ seen' = [v \in VerIds |-> Min(Up(seen[v]), RemoteLease)]
  /\ stamp' = IF Ctl = "LeaseFromStamp" THEN [v \in VerIds |-> Up(stamp[v])] ELSE stamp
  /\ rage' = [r \in RunIds |-> IF runSt[r] = "live" THEN Min(Up(rage[r]), Grace) ELSE -1]
  /\ fl' = IF UseFollower THEN [fl EXCEPT !.age = Min(@ + 1, Tpin)] ELSE fl   \* only "younger than T_pin" matters
  /\ rd' = [x \in Readers |->
              IF rd[x].ph = "held"
              THEN IF rd[x].age + 1 >= HoldLife
                   THEN [rd[x] EXCEPT !.age = 0, !.ph = "revoked"]
                   ELSE [rd[x] EXCEPT !.age = @ + 1]
              ELSE rd[x]]
  /\ job' = IF job # NoJob /\ Ctl = "LeaseFromStamp" THEN [job EXCEPT !.st = Min(@ + 1, Cap)] ELSE job
  /\ <<seen', stamp', rage', fl', rd', job'>> # <<seen, stamp, rage, fl, rd, job>>
  /\ UNCHANGED <<seq, hist, tr, eenv, mem, l0, sr, flushed, ver, runSt, runRows,
                 nComp, nMerge, ck, fk, crashes, bad>>

Next ==
  \/ \E t \in {"put", "del", "erase"}, k \in Keys : Write(t, k)
  \/ EnableE1 \/ Flush
  \/ Absorb \/ DeltaMerge \/ Reseal \/ GroupMerge \/ PublishLand
  \/ OwnerDrop
  \/ ~Lean /\ (PublishLost \/ RemoteDrop \/ GC)
  \/ \E x \in Readers : RBegin(x) \/ RStep(x) \/ RAbort(x) \/ REnd(x)
  \/ FRefresh \/ FStart \/ FStep
  \/ CkBegin \/ CkTake \/ Fork \/ \E t \in {"put", "del"}, k \in Keys : ForkWrite(t, k)
  \/ Crash \/ Tick

Spec == Init /\ [][Next]_vars

-----------------------------------------------------------------------------
(* Invariants.                                                             *)

TypeOK ==
  /\ seq \in 0..MaxSeq /\ NV \in 1..MaxVer
  /\ \A r \in RunIds : runSt[r] \in {"free", "live", "gone"}

\* readers (holds and the follower) never see a wrong value or a deleted run
NoBadRead == bad = {}

\* the newest version plus SlateDB answers every key as of the last write
LatestReadOK ==
  /\ RunsLive(Latest)
  /\ \A k \in Keys : Read(Latest, SlateRows, seq, k) = tr[seq][k]

\* the checkpoint read through its paired version (export, PITR diff) equals
\* the database at last_l0_seq
CkptReadOK ==
  ck.st = "taken" =>
    /\ RunsLive(ver[ck.v])
    /\ ver[ck.v].xd <= ck.cs
    /\ \A k \in Keys : Read(ver[ck.v], ck.rows, ck.cs, k) = tr[ck.cs][k]

\* the fork reads as the parent at the checkpoint plus its own write
ForkReadOK ==
  fk.on =>
    /\ RunsLive(fk.v)
    /\ \A k \in Keys : Read(fk.v, fk.rows, fk.seq, k) = fk.tr[k]

\* every stored row decodes with its database's E_env (envelope iff seq > E_env)
DecodeOK(R, e) == \A r \in R : (r.s > e) = r.enc
EncodingOK ==
  /\ DecodeOK(SlateRows \cup RowsOfRuns({r \in RunIds : runSt[r] = "live"}), eenv)
  /\ fk.on => DecodeOK(fk.rows \cup RowsOfRuns(RunsOf(fk.v)), fk.eenv)

\* I2: X_g <= X_d, and the X_g of every key's group never goes down
XgOK ==
  /\ \A v \in 1..NV : \A g \in Groups : ver[v].xg[g] <= ver[v].xd
  /\ \A v \in 1..(NV - 1) : \A k \in Keys :
       ver[v + 1].xg[ver[v + 1].own[k]] >= ver[v].xg[ver[v].own[k]]

\* the erasure range rule: a sealed run never holds a put that an erasure at or
\* below its X_g covers
ErasedGone ==
  \A g \in Groups : \A r \in SealedRows(Latest, g) :
     ~\E s \in 1..seq : hist[s].t = "erase" /\ hist[s].k = Cust[r.k] /\ r.s < s /\ s <= Latest.xg[g]

=============================================================================
