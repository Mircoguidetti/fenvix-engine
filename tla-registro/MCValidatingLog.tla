---- MODULE MCValidatingLog ----
EXTENDS ValidatingLog
CONSTANTS r1, r2, r3, k1, k2, k3, t1, t2, t3
Sym == Permutations({r1, r2, r3})
\* t1: $inc su k1 (delta cieco, dipende solo dall'esistenza di k1)
\* t2: cancellazione/sostituzione di k1 (strutturale) e $inc su k3
\* t3: findAndModify che legge k1 (valore) e scrive k2 con un delta
MCDeps == ( t1 :> {[k |-> k1, mode |-> "esiste"]}
         @@ t2 :> {[k |-> k1, mode |-> "esiste"], [k |-> k3, mode |-> "esiste"]}
         @@ t3 :> {[k |-> k1, mode |-> "valore"], [k |-> k2, mode |-> "esiste"]} )
MCWrites == ( t1 :> {[k |-> k1, kind |-> "delta"]}
           @@ t2 :> {[k |-> k1, kind |-> "strutturale"], [k |-> k3, kind |-> "delta"]}
           @@ t3 :> {[k |-> k2, kind |-> "delta"]} )
====
