---- MODULE MCBollettino ----
EXTENDS Bollettino
CONSTANTS r1, r2, r3, k1, k2, t1, t2, d1, d2
Sym == Permutations({r1, r2, r3})
\* t1: $inc su k1 (database d1); t2: inserimento di k2 (database d2)
MCDeps == ( t1 :> {[k |-> k1, mode |-> "esiste"]}
         @@ t2 :> {[k |-> k2, mode |-> "esiste"]} )
MCWrites == ( t1 :> {[k |-> k1, kind |-> "delta"]}
           @@ t2 :> {[k |-> k2, kind |-> "strutturale"]} )
MCDbOf == (k1 :> d1 @@ k2 :> d2)
====
