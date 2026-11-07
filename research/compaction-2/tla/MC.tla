---------------------------------- MODULE MC ----------------------------------
EXTENDS Compaction2
CONSTANTS a1, a2, b1                 \* keys (model values): a1, a2 of customer A, b1 of B
MCKeys   == {a1, a2, b1}
MCGroups == {"g1", "g2"}
MCCust   == [k \in MCKeys |-> IF k = b1 THEN "B" ELSE "A"]
MCGrp0   == [k \in MCKeys |-> IF k = b1 THEN "g2" ELSE "g1"]
\* a1 and a2 are interchangeable (same customer, same group)
MCSym    == Permutations({a1, a2})
\* A time budget for a configuration whose state space does not close: TLC stops
\* exploring after StopAfter seconds and prints its statistics (0 = no budget).
CONSTANT StopAfter
MCBudget == StopAfter = 0 \/ TLCGet("duration") < StopAfter \/ TLCSet("exit", TRUE)
===============================================================================
