---------------------------- MODULE Bollettino ----------------------------
(***************************************************************************)
(* Il bollettino (docs/protocollo.md §6.4): il quaderno avvisa il cameriere *)
(* (nodo d'ingresso) di cio' che cambia, e il cameriere legge senza         *)
(* chiedere al quaderno quando sa che una chiave non ha voci sopra M.       *)
(*                                                                          *)
(* Sopra ValidatingLog, con:                                                *)
(*  - piu' database (DbOf: chiave -> database), M per database (Mdb), lo    *)
(*    scarico astratto (Flush: la correttezza dello scarico e' gia'         *)
(*    verificata in LettureScarico) e il disco che si svuota (TrimR) fino   *)
(*    alla prima voce non scaricata di qualsiasi database;                  *)
(*  - la ricarica da S3 (Reload) e il riavvio di una replica (Restart:      *)
(*    confermataLocale torna a 0, il disco resta);                          *)
(*  - un cameriere: si iscrive a una replica (fotografia), riceve gli       *)
(*    annunci, si stacca, adotta manifest nuovi, legge senza chiedere.      *)
(*                                                                          *)
(* Stato del cameriere: sapere (sapereFinoA), ultima[d], toccate[k] (la     *)
(* posizione piu' alta annunciata per chiave), Mv[d] (la sua vista del      *)
(* manifest), sub (replica a cui e' iscritto o NoSub).                      *)
(* Stato dell'iscrizione: ann = indice fino al quale la replica ha          *)
(* annunciato il proprio log al cameriere.                                  *)
(*                                                                          *)
(* Controlli negativi (correzioni del revisore di letture e affinita' e del *)
(* revisore di S1):                                                         *)
(*  FattoSottoS           si legge senza chiedere a una S piu' alta di cio' *)
(*                        che il cameriere sa                               *)
(*  AnnuncioDopoConferma  la replica comunica la confermata prima di        *)
(*                        annunciare le voci che copre                      *)
(*  FotografiaSoloFinestra la fotografia guarda solo la finestra in RAM     *)
(*  FotoSenzaM            la fotografia guarda solo il disco e non dice     *)
(*                        fino a dove il database e' scaricato              *)
(*  SvuotaChiaviAlManifest al manifest nuovo il cameriere svuota tutte le   *)
(*                        chiavi del database invece di potare per posizione*)
(*  ScartaAnnunciVecchi   il cameriere butta i toccati degli annunci con    *)
(*                        confermataLocale piu' bassa di cio' che sa        *)
(*  FotoSostituisce       alla nuova iscrizione la fotografia sostituisce i *)
(*                        fatti invece di unirsi                            *)
(*  SMinoreDiM            non si applica S := max(S, M)                     *)
(*  CapoNuovoSenzaAnnunci le voci nuove scritte al posto di voci troncate   *)
(*                        (cambio di capo) non vengono riannunciate         *)
(***************************************************************************)
EXTENDS ValidatingLog

CONSTANTS DbOf, Dbs, MaxSkipReads, MaxFlushes,
          FattoSottoS, AnnuncioDopoConferma, FotografiaSoloFinestra, FotoSenzaM,
          SvuotaChiaviAlManifest, ScartaAnnunciVecchi, FotoSostituisce, SMinoreDiM,
          CapoNuovoSenzaAnnunci

NoSub == "nessuna"

VARIABLES
  Mdb,      \* database -> posizione fino alla quale e' su S3 (manifest corrente)
  flushes,  \* numero di scarichi (limite del modello)
  sub,      \* replica a cui il cameriere e' iscritto, o NoSub
  ann,      \* indice fino al quale la replica sub ha annunciato il proprio log
  sapere,   \* sapereFinoA del cameriere
  ultima,   \* database -> posizione piu' alta annunciata (0 = nessuna)
  toccate,  \* chiave -> posizione piu' alta annunciata (0 = nessuna)
  Mv,       \* database -> vista del manifest del cameriere
  skips,    \* letture senza domanda servite (limite del modello)
  readOk    \* FALSE appena una lettura senza domanda e' sbagliata

bvars == <<Mdb, flushes, sub, ann, sapere, ultima, toccate, Mv, skips, readOk>>
allvars == <<vars, bvars>>

Max2(a, b) == IF a > b THEN a ELSE b
KeysOf(en) == {w.k : w \in WritesOf(en)}
DbsOf(en) == {DbOf[k] : k \in KeysOf(en)}

\* prima voce confermata non ancora scaricata, su tutti i database (le barriere non contano)
FirstUnflushed ==
  LET U == {i \in 1..Len(cmtLog) : \E d \in DbsOf(cmtLog[i]) : i > Mdb[d]}
  IN IF U = {} THEN Len(cmtLog) + 1 ELSE CHOOSE i \in U : \A j \in U : i <= j

\* posizione piu' alta di una voce del database d nel log di r da "from" in su (0 = nessuna)
LastOn(r, d, from) ==
  LET P == {i \in from..Len(log[r]) : d \in DbsOf(log[r][i])}
  IN IF P = {} THEN 0 ELSE Max(P)

\* scritture confermate alla chiave k fino alla posizione u
WritesK(k, u) == {i \in 1..Min(u, Len(cmtLog)) : k \in KeysOf(cmtLog[i])}

BInit ==
  /\ Init
  /\ Mdb = [d \in Dbs |-> 0]
  /\ flushes = 0
  /\ sub = NoSub
  /\ ann = 0
  /\ sapere = 0
  /\ ultima = [d \in Dbs |-> 0]
  /\ toccate = [k \in Keys |-> 0]
  /\ Mv = [d \in Dbs |-> 0]
  /\ skips = 0
  /\ readOk = TRUE

(* --------------------------------------------- registro, con il bollettino *)

\* se il log della replica iscritta cambia sotto ann (troncamento, ricarica), la replica deve
\* riannunciare da li': ann scende al prefisso comune tra log vecchio e nuovo
AnnAfter(r) ==
  IF sub = r /\ ~CapoNuovoSenzaAnnunci THEN Min(ann, MatchLen(log[r], log'[r])) ELSE ann

BElect(c, Q) == Elect(c, Q) /\ ann' = AnnAfter(c) /\ UNCHANGED <<Mdb, flushes, sub, sapere, ultima, toccate, Mv, skips, readOk>>
BSubmit(l, t) == Submit(l, t) /\ UNCHANGED bvars
BReplicate(l, f, p, n) ==
  Replicate(l, f, p, n) /\ ann' = AnnAfter(f) /\ UNCHANGED <<Mdb, flushes, sub, sapere, ultima, toccate, Mv, skips, readOk>>
BAdvanceCommit(l) == AdvanceCommit(l) /\ UNCHANGED bvars
BAdvanceWindow(l) == AdvanceWindow(l) /\ UNCHANGED bvars

\* scarico astratto del database d fino a una posizione confermata
Flush(d) ==
  /\ flushes < MaxFlushes
  /\ \E p \in (Mdb[d] + 1)..Len(cmtLog) : Mdb' = [Mdb EXCEPT ![d] = p]
  /\ flushes' = flushes + 1
  /\ UNCHANGED vars
  /\ UNCHANGED <<sub, ann, sapere, ultima, toccate, Mv, skips, readOk>>

\* la replica cancella dal disco fino alla prima voce non scaricata (mai sopra la propria finestra)
TrimR(r) ==
  /\ alive[r]
  /\ \E d \in (diskStart[r] + 1)..Min(FirstUnflushed, wstart[r]) : diskStart' = [diskStart EXCEPT ![r] = d]
  /\ UNCHANGED <<epoch, role, alive, log, commit, match, wstart, mapS, mapA, ids, attempts, outcome, cmtLog, losses>>
  /\ UNCHANGED bvars

\* ricarica da S3 di un seguace rimasto sotto l'inizioDisco del capo
Reload(f) ==
  /\ alive[f] /\ role[f] = "follower"
  /\ \E l \in Replicas :
       /\ alive[l] /\ role[l] = "leader" /\ epoch[l] >= epoch[f]
       /\ MatchLen(log[l], log[f]) + 1 < diskStart[l]
       /\ LET P == diskStart[l] - 1
              keep == Len(log[f]) >= P /\ log[f][P] = cmtLog[P]
              newLog == IF keep THEN SubSeq(cmtLog, 1, P) \o SubSeq(log[f], P + 1, Len(log[f]))
                        ELSE SubSeq(cmtLog, 1, P)
          IN /\ P + 1 > diskStart[f]
             /\ log' = [log EXCEPT ![f] = newLog]
             /\ diskStart' = [diskStart EXCEPT ![f] = P + 1]
             /\ wstart' = [wstart EXCEPT ![f] = Max2(@, P + 1)]
             /\ commit' = [commit EXCEPT ![f] = Max2(@, P)]
  /\ ann' = AnnAfter(f)
  /\ UNCHANGED <<epoch, role, alive, match, mapS, mapA, ids, attempts, outcome, cmtLog, losses>>
  /\ UNCHANGED <<Mdb, flushes, sub, sapere, ultima, toccate, Mv, skips, readOk>>

\* la replica riparte con il disco intatto: confermataLocale a 0 (protocollo §8)
Restart(r) ==
  /\ alive[r] /\ role[r] = "follower" /\ commit[r] > 0
  /\ commit' = [commit EXCEPT ![r] = 0]
  /\ UNCHANGED <<epoch, role, alive, log, match, wstart, mapS, mapA, ids, attempts, outcome, cmtLog, losses, diskStart>>
  /\ UNCHANGED bvars

(* ------------------------------------------------------------- bollettino *)

\* iscrizione: fotografia. Per ogni database: la posizione piu' alta di una sua voce sul disco
\* (finestra E disco, anche non confermata) e fino a dove e' scaricato (Mdb, che la replica conosce
\* da SCARICATO). Il cameriere UNISCE la fotografia ai fatti che ha; le chiavi non ci sono, quindi
\* ogni chiave di un database con qualcosa sopra la vista del manifest conta come toccata.
Subscribe(r) ==
  /\ alive[r] /\ sub /= r
  /\ LET from == IF FotografiaSoloFinestra THEN wstart[r] ELSE diskStart[r]
         photo == [d \in Dbs |-> IF FotoSenzaM THEN LastOn(r, d, from) ELSE Max2(LastOn(r, d, from), Mdb[d])]
     IN /\ ultima' = [d \in Dbs |-> IF FotoSostituisce THEN photo[d] ELSE Max2(ultima[d], photo[d])]
        /\ toccate' = [k \in Keys |-> IF FotoSostituisce THEN photo[DbOf[k]] ELSE Max2(toccate[k], photo[DbOf[k]])]
        /\ sapere' = IF FotoSostituisce THEN commit[r] ELSE Max2(sapere, commit[r])
  /\ sub' = r
  /\ ann' = Len(log[r])
  /\ UNCHANGED vars
  /\ UNCHANGED <<Mdb, flushes, Mv, skips, readOk>>

Unsubscribe ==
  /\ sub /= NoSub
  /\ sub' = NoSub
  /\ ann' = 0
  /\ UNCHANGED vars
  /\ UNCHANGED <<Mdb, flushes, sapere, ultima, toccate, Mv, skips, readOk>>

\* la replica annuncia la voce successiva del proprio log (confermata o no)
Announce ==
  /\ sub /= NoSub /\ alive[sub] /\ ann < Len(log[sub])
  /\ LET i == ann + 1
         en == log[sub][i]
         drop == ScartaAnnunciVecchi /\ commit[sub] < sapere
     IN /\ ultima' = [d \in Dbs |-> IF ~drop /\ d \in DbsOf(en) THEN Max2(ultima[d], i) ELSE ultima[d]]
        /\ toccate' = [k \in Keys |-> IF ~drop /\ k \in KeysOf(en) THEN Max2(toccate[k], i) ELSE toccate[k]]
        /\ ann' = i
  /\ UNCHANGED vars
  /\ UNCHANGED <<Mdb, flushes, sub, sapere, Mv, skips, readOk>>

\* la replica comunica la propria confermataLocale, solo fino a cio' che ha gia' annunciato
AnnounceCommit ==
  /\ sub /= NoSub /\ alive[sub]
  /\ LET c == IF AnnuncioDopoConferma THEN commit[sub] ELSE Min(commit[sub], ann)
     IN /\ c > sapere
        /\ sapere' = c
  /\ UNCHANGED vars
  /\ UNCHANGED <<Mdb, flushes, sub, ann, ultima, toccate, Mv, skips, readOk>>

\* il cameriere adotta il manifest corrente di d e pota le chiavi con posizione fino al nuovo M
AdoptManifest(d) ==
  /\ Mv[d] < Mdb[d]
  /\ Mv' = [Mv EXCEPT ![d] = Mdb[d]]
  /\ toccate' = [k \in Keys |-> IF DbOf[k] = d /\ (SvuotaChiaviAlManifest \/ toccate[k] <= Mdb[d]) THEN 0 ELSE toccate[k]]
  /\ UNCHANGED vars
  /\ UNCHANGED <<Mdb, flushes, sub, ann, sapere, ultima, skips, readOk>>

\* lettura della chiave k a S = s senza domanda al quaderno: il valore e' quello delle scatole a Mv
\* (le scritture confermate alla chiave fino a Mv). Deve coincidere con la lettura completa a s.
SkipRead(k, s) ==
  /\ skips < MaxSkipReads
  /\ LET d == DbOf[k]
         sEff == IF SMinoreDiM THEN s ELSE Max2(s, Mv[d])
     IN /\ FattoSottoS \/ sEff <= sapere
        /\ sEff <= Len(cmtLog)
        /\ toccate[k] <= Mv[d]
        /\ readOk' = (readOk /\ WritesK(k, Mv[d]) = WritesK(k, sEff))
  /\ skips' = skips + 1
  /\ UNCHANGED vars
  /\ UNCHANGED <<Mdb, flushes, sub, ann, sapere, ultima, toccate, Mv>>

BNext ==
  \/ \E c \in Replicas, Q \in SUBSET Replicas : BElect(c, Q)
  \/ \E l \in Replicas, t \in Txns : BSubmit(l, t)
  \/ \E l, f \in Replicas, p \in 0..MaxLog, n \in 0..1 : BReplicate(l, f, p, n)
  \/ \E l \in Replicas : BAdvanceCommit(l) \/ BAdvanceWindow(l) \/ TrimR(l) \/ Reload(l) \/ Restart(l)
  \/ \E d \in Dbs : Flush(d) \/ AdoptManifest(d)
  \/ \E r \in Replicas : Subscribe(r)
  \/ Unsubscribe \/ Announce \/ AnnounceCommit
  \/ \E k \in Keys, s \in 0..MaxCommit : SkipRead(k, s)

BSpec == BInit /\ [][BNext]_allvars

(* ------------------------------------------------------------- invarianti *)

\* una lettura senza domanda da' lo stesso risultato della lettura completa alla stessa S
ReadsCorrect == readOk

\* un fatto del bollettino e' vero: nessuna voce confermata del database d tra la vista del manifest
\* e la posizione fino a cui il cameriere dice di sapere tutto, se non l'ha saputa
BollettinoVero ==
  \A d \in Dbs :
    ultima[d] <= Mv[d] =>
      \A i \in (Mv[d] + 1)..Min(sapere, Len(cmtLog)) : d \notin DbsOf(cmtLog[i])
ChiaviVere ==
  \A k \in Keys :
    \A i \in (Mv[DbOf[k]] + 1)..Min(sapere, Len(cmtLog)) :
      k \in KeysOf(cmtLog[i]) => toccate[k] >= i

\* cio' che il cameriere sa e' confermato
SapereConfermato == sapere <= Len(cmtLog)

\* lo scarico astratto e il disco: nulla cancellato che non sia su S3
NothingLost == \A r \in Replicas : diskStart[r] <= FirstUnflushed

=============================================================================
