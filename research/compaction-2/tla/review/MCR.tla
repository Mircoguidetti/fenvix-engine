---------------------------------- MODULE MCR ---------------------------------
\* The review's model values: as MC.tla, plus a layout where customer A spans both groups.
EXTENDS Compaction2R
CONSTANTS a1, a2, b1
MCKeys   == {a1, a2, b1}
MCGroups == {"g1", "g2"}
MCCust   == [k \in MCKeys |-> IF k = b1 THEN "B" ELSE "A"]
MCGrp0   == [k \in MCKeys |-> IF k = b1 THEN "g2" ELSE "g1"]
\* a whale customer A over two groups: a1 in g1, a2 (its tail) in g2 with B
MCGrp0Span == [k \in MCKeys |-> IF k = a1 THEN "g1" ELSE "g2"]
MCSym    == Permutations({a1, a2})
CONSTANT StopAfter
MCBudget == StopAfter = 0 \/ TLCGet("duration") < StopAfter \/ TLCSet("exit", TRUE)
===============================================================================
