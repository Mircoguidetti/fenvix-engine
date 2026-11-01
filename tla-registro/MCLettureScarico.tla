---- MODULE MCLettureScarico ----
EXTENDS LettureScarico
CONSTANTS r1, r2, r3, k1, k2, k3, t1, t2, t3, c1, c2, w1, w2
Sym == Permutations({r1, r2, r3}) \cup Permutations({w1, w2})
MCDeps == ( t1 :> {[k |-> k1, mode |-> "esiste"]}
         @@ t2 :> {[k |-> k1, mode |-> "esiste"], [k |-> k3, mode |-> "esiste"]}
         @@ t3 :> {[k |-> k1, mode |-> "valore"], [k |-> k2, mode |-> "esiste"]} )
MCWrites == ( t1 :> {[k |-> k1, kind |-> "delta"]}
           @@ t2 :> {[k |-> k1, kind |-> "strutturale"], [k |-> k3, kind |-> "delta"]}
           @@ t3 :> {[k |-> k2, kind |-> "delta"]} )
MCOwner == (t1 :> c1 @@ t2 :> c2 @@ t3 :> c1)
====
