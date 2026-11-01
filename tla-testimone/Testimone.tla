------------------------------ MODULE Testimone ------------------------------
(***************************************************************************)
(* "Due copie con i dati piu' testimone che tiene le ricevute"             *)
(* (in termini veri: replica set PSA con un arbitro che conserva l'ultima  *)
(* posizione confermata in modo degradato).                                 *)
(*                                                                          *)
(* Pezzi:                                                                   *)
(*  - due copie con i dati (Copies, per esempio {a, b}): ognuna ha epoca,   *)
(*    ruolo, modo, quaderno (log = sequenza di epoche: la voce i scritta    *)
(*    dal capo dell'epoca e) e un disco che si puo' perdere (Lose);         *)
(*  - un testimone senza dati: tiene solo wEpoch (l'epoca piu' alta vista)  *)
(*    e wRec, la ricevuta [ep, n] dell'ultima conferma in modo degradato.   *)
(*                                                                          *)
(* Regole del protocollo:                                                   *)
(*  R1 Elezione: un candidato diventa capo all'epoca e con 2 voti su 3:     *)
(*     il suo e quello dell'altra copia oppure del testimone. Ogni votante  *)
(*     vota una volta per epoca (la sua epoca deve essere < e). L'altra     *)
(*     copia vota solo un candidato con il quaderno almeno aggiornato come  *)
(*     il suo (regola di Raft); il testimone solo un candidato con il       *)
(*     quaderno almeno aggiornato come la ricevuta.                          *)
(*  R2 Modo pieno: il capo conferma la posizione n solo quando entrambe le  *)
(*     copie hanno il quaderno fino a n (ack dell'altra copia nell'epoca    *)
(*     corrente del capo).                                                  *)
(*  R3 Modo degradato: il capo ci entra solo se il testimone accetta la sua *)
(*     epoca (wEpoch <= epoca del capo; il testimone la adotta). In modo     *)
(*     degradato conferma da solo la posizione n solo se il testimone e'    *)
(*     ancora alla sua epoca e registra la ricevuta [ep, n] PRIMA della     *)
(*     risposta al client. La voce n deve essere dell'epoca del capo        *)
(*     (regola di Raft sul commit).                                          *)
(*  R4 Il testimone tiene epoca e ricevuta su disco durevole.               *)
(*                                                                          *)
(* Controlli negativi (costanti, tutte FALSE = protocollo giusto):          *)
(*  AckOnOne          R2 tolta: conferma in modo pieno con la sola copia    *)
(*                    del capo, senza ricevuta                              *)
(*  NoWitnessGrant    R3 tolta: il capo entra in modo degradato e conferma  *)
(*                    da solo senza chiedere niente al testimone            *)
(*  NoReceipt         il testimone da' il permesso ma non scrive ricevute   *)
(*  NoReceiptCheck    il testimone vota senza guardare la ricevuta          *)
(*  WitnessDoubleVote il testimone vota due volte nella stessa epoca        *)
(*  WitnessAmnesia    R4 tolta: il testimone puo' ripartire con epoca e     *)
(*                    ricevuta azzerate (disco del testimone perso)         *)
(*  NoOwnEpochRule    in modo degradato si confermano anche voci di epoche  *)
(*                    vecchie (regola di Raft sul commit tolta)             *)
(***************************************************************************)
EXTENDS Naturals, Sequences, FiniteSets

CONSTANTS Copies, MaxEpoch, MaxLen, MaxLoss, MaxRestart,
          AckOnOne, NoWitnessGrant, NoReceipt, NoReceiptCheck,
          WitnessDoubleVote, WitnessAmnesia, NoOwnEpochRule

ASSUME Cardinality(Copies) = 2

VARIABLES
  epoch,     \* copia -> epoca piu' alta vista (su disco)
  role,      \* copia -> "capo" | "seguace"
  mode,      \* copia -> "pieno" | "degradato" (conta solo per il capo)
  log,       \* copia -> quaderno: sequenza di epoche
  alive,     \* copia -> FALSE = disco perso, non agisce e non vota piu'
  match,     \* copia -> lunghezza del quaderno dell'altra copia confermata (ack) nell'epoca corrente
  wEpoch,    \* testimone: epoca piu' alta vista
  wRec,      \* testimone: ricevuta [ep, n] dell'ultima conferma in modo degradato
  cmt,       \* fantasma: il prefisso confermato piu' lungo mai visto dal client
  cmtAt,     \* fantasma: per ogni posizione di cmt, l'epoca del capo che l'ha confermata per primo
  pienoMax,  \* fantasma: la posizione piu' alta confermata in modo pieno
  split,     \* fantasma: TRUE se due conferme sono in conflitto (cervello diviso)
  degBy,     \* fantasma: le copie che hanno confermato almeno una volta in modo degradato
  losses,    \* numero di dischi persi
  restarts   \* numero di ripartenze del testimone senza memoria (solo WitnessAmnesia)

vars == <<epoch, role, mode, log, alive, match, wEpoch, wRec, cmt, cmtAt, pienoMax, split, degBy, losses, restarts>>

Other(c) == CHOOSE d \in Copies : d # c
LastEp(s) == IF Len(s) = 0 THEN 0 ELSE s[Len(s)]
\* il quaderno s e' almeno aggiornato come la posizione [ep, n] (confronto di Raft)
AtLeast(s, ep, n) == LastEp(s) > ep \/ (LastEp(s) = ep /\ Len(s) >= n)
IsPrefix(p, s) == Len(p) <= Len(s) /\ SubSeq(s, 1, Len(p)) = p
Rec(ep, n) == [ep |-> ep, n |-> n]
NoRec == Rec(0, 0)

TypeOK ==
  /\ epoch \in [Copies -> 0..MaxEpoch]
  /\ role \in [Copies -> {"capo", "seguace"}]
  /\ mode \in [Copies -> {"pieno", "degradato"}]
  /\ alive \in [Copies -> BOOLEAN]
  /\ match \in [Copies -> 0..MaxLen]
  /\ wEpoch \in 0..MaxEpoch
  /\ wRec \in [ep : 0..MaxEpoch, n : 0..MaxLen]
  /\ split \in BOOLEAN
  /\ pienoMax \in 0..MaxLen

Init ==
  /\ epoch = [c \in Copies |-> 0]
  /\ role = [c \in Copies |-> "seguace"]
  /\ mode = [c \in Copies |-> "pieno"]
  /\ log = [c \in Copies |-> <<>>]
  /\ alive = [c \in Copies |-> TRUE]
  /\ match = [c \in Copies |-> 0]
  /\ wEpoch = 0
  /\ wRec = NoRec
  /\ cmt = <<>>
  /\ cmtAt = <<>>
  /\ pienoMax = 0
  /\ split = FALSE
  /\ degBy = {}
  /\ losses = 0
  /\ restarts = 0

\* Il client riceve "confermato" per il prefisso p dal capo all'epoca e.
Confirm(p, e) ==
  IF IsPrefix(p, cmt) THEN UNCHANGED <<cmt, cmtAt, split>>
  ELSE IF IsPrefix(cmt, p)
    THEN /\ cmt' = p
         /\ cmtAt' = [i \in 1..Len(p) |-> IF i <= Len(cmt) THEN cmtAt[i] ELSE e]
         /\ UNCHANGED split
  ELSE split' = TRUE /\ UNCHANGED <<cmt, cmtAt>>

-----------------------------------------------------------------------------
(* R1: elezione con il voto dell'altra copia *)
ElectByCopy(c, e) ==
  LET v == Other(c) IN
  /\ alive[c] /\ alive[v]
  /\ epoch[c] < e /\ epoch[v] < e
  /\ AtLeast(log[c], LastEp(log[v]), Len(log[v]))
  /\ epoch' = [epoch EXCEPT ![c] = e, ![v] = e]
  /\ role' = [role EXCEPT ![c] = "capo", ![v] = "seguace"]
  /\ mode' = [mode EXCEPT ![c] = "pieno", ![v] = "pieno"]
  /\ match' = [match EXCEPT ![c] = 0]
  /\ UNCHANGED <<log, alive, wEpoch, wRec, cmt, cmtAt, pienoMax, split, degBy, losses, restarts>>

(* R1: elezione con il voto del testimone *)
ElectByWitness(c, e) ==
  /\ alive[c]
  /\ epoch[c] < e
  /\ IF WitnessDoubleVote THEN wEpoch <= e ELSE wEpoch < e
  /\ NoReceiptCheck \/ AtLeast(log[c], wRec.ep, wRec.n)
  /\ wEpoch' = e
  /\ epoch' = [epoch EXCEPT ![c] = e]
  /\ role' = [role EXCEPT ![c] = "capo"]
  /\ mode' = [mode EXCEPT ![c] = "pieno"]
  /\ match' = [match EXCEPT ![c] = 0]
  /\ UNCHANGED <<log, alive, wRec, cmt, cmtAt, pienoMax, split, degBy, losses, restarts>>

(* Il capo scrive una voce nuova nel suo quaderno (non ancora confermata). *)
ClientWrite(l) ==
  /\ alive[l] /\ role[l] = "capo"
  /\ Len(log[l]) < MaxLen
  /\ log' = [log EXCEPT ![l] = Append(@, epoch[l])]
  /\ UNCHANGED <<epoch, role, mode, alive, match, wEpoch, wRec, cmt, cmtAt, pienoMax, split, degBy, losses, restarts>>

(* Il capo porta l'altra copia in pari (copia intera del quaderno, come     *)
(* dopo il troncamento dei conflitti di Raft); l'altra copia adotta         *)
(* l'epoca, diventa seguace e risponde con l'ack.                           *)
Replicate(l) ==
  LET f == Other(l) IN
  /\ alive[l] /\ alive[f] /\ role[l] = "capo"
  /\ epoch[f] <= epoch[l]
  /\ log[f] # log[l] \/ match[l] # Len(log[l])
  /\ log' = [log EXCEPT ![f] = log[l]]
  /\ epoch' = [epoch EXCEPT ![f] = epoch[l]]
  /\ role' = [role EXCEPT ![f] = "seguace"]
  /\ mode' = [mode EXCEPT ![f] = "pieno"]
  /\ match' = [match EXCEPT ![l] = Len(log[l])]
  /\ UNCHANGED <<alive, wEpoch, wRec, cmt, cmtAt, pienoMax, split, degBy, losses, restarts>>

(* R2: conferma in modo pieno della posizione n *)
CommitPieno(l, n) ==
  /\ alive[l] /\ role[l] = "capo"
  /\ n \in 1..Len(log[l])
  /\ AckOnOne \/ n <= match[l]
  /\ Confirm(SubSeq(log[l], 1, n), epoch[l])
  /\ pienoMax' = IF n > pienoMax THEN n ELSE pienoMax
  /\ UNCHANGED <<epoch, role, mode, log, alive, match, wEpoch, wRec, degBy, losses, restarts>>

(* R3: ingresso nel modo degradato, con il permesso del testimone *)
EnterDegraded(l) ==
  /\ alive[l] /\ role[l] = "capo" /\ mode[l] = "pieno"
  /\ IF NoWitnessGrant THEN UNCHANGED wEpoch
     ELSE wEpoch <= epoch[l] /\ wEpoch' = epoch[l]
  /\ mode' = [mode EXCEPT ![l] = "degradato"]
  /\ UNCHANGED <<epoch, role, log, alive, match, wRec, cmt, cmtAt, pienoMax, split, degBy, losses, restarts>>

(* Uscita dal modo degradato: il capo torna a confermare solo con entrambe. *)
LeaveDegraded(l) ==
  /\ alive[l] /\ role[l] = "capo" /\ mode[l] = "degradato"
  /\ mode' = [mode EXCEPT ![l] = "pieno"]
  /\ UNCHANGED <<epoch, role, log, alive, match, wEpoch, wRec, cmt, cmtAt, pienoMax, split, degBy, losses, restarts>>

(* R3: conferma in modo degradato della posizione n, con ricevuta *)
CommitDegraded(l, n) ==
  /\ alive[l] /\ role[l] = "capo" /\ mode[l] = "degradato"
  /\ n \in 1..Len(log[l])
  /\ NoOwnEpochRule \/ log[l][n] = epoch[l]
  /\ IF NoWitnessGrant \/ NoReceipt
       THEN /\ (NoWitnessGrant \/ wEpoch = epoch[l])
            /\ UNCHANGED wRec
       ELSE /\ wEpoch = epoch[l]
            /\ wRec' = IF AtLeast(SubSeq(log[l], 1, n), wRec.ep, wRec.n)
                         THEN Rec(log[l][n], n) ELSE wRec
  /\ Confirm(SubSeq(log[l], 1, n), epoch[l])
  /\ degBy' = degBy \cup {l}
  /\ UNCHANGED <<epoch, role, mode, log, alive, match, wEpoch, pienoMax, losses, restarts>>

(* Una copia riparte (processo morto, disco intatto): perde il ruolo. *)
Restart(c) ==
  /\ alive[c] /\ role[c] = "capo"
  /\ role' = [role EXCEPT ![c] = "seguace"]
  /\ mode' = [mode EXCEPT ![c] = "pieno"]
  /\ UNCHANGED <<epoch, log, alive, match, wEpoch, wRec, cmt, cmtAt, pienoMax, split, degBy, losses, restarts>>

(* Una copia perde il disco: non torna piu'. *)
Lose(c) ==
  /\ alive[c] /\ losses < MaxLoss
  /\ alive' = [alive EXCEPT ![c] = FALSE]
  /\ losses' = losses + 1
  /\ UNCHANGED <<epoch, role, mode, log, match, wEpoch, wRec, cmt, cmtAt, pienoMax, split, degBy, restarts>>

(* Solo controllo negativo: il testimone perde il disco e riparte vuoto. *)
WitnessReset ==
  /\ WitnessAmnesia /\ restarts < MaxRestart
  /\ wEpoch' = 0 /\ wRec' = NoRec
  /\ restarts' = restarts + 1
  /\ UNCHANGED <<epoch, role, mode, log, alive, match, cmt, cmtAt, pienoMax, split, degBy, losses>>

Next ==
  \/ \E c \in Copies, e \in 1..MaxEpoch : ElectByCopy(c, e) \/ ElectByWitness(c, e)
  \/ \E c \in Copies : ClientWrite(c) \/ Replicate(c) \/ EnterDegraded(c)
                       \/ LeaveDegraded(c) \/ Restart(c) \/ Lose(c)
  \/ \E c \in Copies, n \in 1..MaxLen : CommitPieno(c, n) \/ CommitDegraded(c, n)
  \/ WitnessReset

Spec == Init /\ [][Next]_vars

-----------------------------------------------------------------------------
(* Invarianti *)

\* Mai due capi nella stessa epoca.
ElectionSafety ==
  \A c, d \in Copies : (c # d /\ role[c] = "capo" /\ role[d] = "capo") => epoch[c] # epoch[d]

\* Nessun cervello diviso: tutte le conferme date ai client stanno su una sola storia.
NoSplitBrain == ~split

\* Una copia rimasta indietro non puo' comandare: ogni capo vivo ha nel quaderno
\* tutte le voci confermate da capi di epoche non piu' alte della sua
\* (le conferme di epoche piu' alte arrivano dopo la sua elezione: e' un capo vecchio).
LeaderCompleteness ==
  \A l \in Copies :
    (alive[l] /\ role[l] = "capo") =>
      \A i \in 1..Len(cmt) :
        cmtAt[i] <= epoch[l] => (i <= Len(log[l]) /\ log[l][i] = cmt[i])

\* Conferma solo con entrambe: cio' che e' confermato in modo pieno sta su ogni copia viva.
DurabilityPieno ==
  \A c \in Copies :
    alive[c] => (pienoMax <= Len(log[c]) /\ SubSeq(log[c], 1, pienoMax) = SubSeq(cmt, 1, pienoMax))

\* Controlli di raggiungibilita' (DEVONO essere violati: il modello non e' vuoto).
SanityNoDegradedCommit == degBy = {}
\* cambio di capo vero dopo il modo degradato: l'altra copia comanda dopo
\* l'ultima conferma, e la copia che confermava da sola ha perso il disco
SanityNoFailover ==
  ~(\E c \in Copies :
      /\ alive[c] /\ role[c] = "capo" /\ ~alive[Other(c)]
      /\ Other(c) \in degBy /\ c \notin degBy
      /\ Len(cmt) >= 1 /\ cmtAt[Len(cmt)] < epoch[c])
=============================================================================
