------------------------------ MODULE Famiglia ------------------------------
(***************************************************************************)
(* Raccolta di famiglia delle scatole (SST) condivise tra rami (fork) e    *)
(* punti di ripristino (PITR). Vedi docs/tla-rami.md.                      *)
(*                                                                          *)
(* Oggetti su S3 modellati:                                                 *)
(*  - scatole (SST) immutabili, ognuna con un proprietario (prefisso);      *)
(*  - un foglio (manifest) per database: l'insieme delle scatole che legge; *)
(*  - il registro della famiglia: un solo oggetto con i membri e le         *)
(*    "spille" (pin): fork in corso e punti di ripristino, ognuna con       *)
(*    l'insieme di scatole che trattiene.                                   *)
(*                                                                          *)
(* Regole del disegno (rapporto revisore-database-rami, punto 2):           *)
(*  1. uno solo cancella: il lavaggio (compattazione) toglie scatole dal    *)
(*     foglio e non cancella mai; cancella solo il raccoglitore;            *)
(*  2. prima si registra, poi si condivide: il fork scrive la spilla nel    *)
(*     registro (atomica con la lettura del foglio sorgente) prima di       *)
(*     scrivere il foglio del ramo;                                         *)
(*  3. eta' minima: una scatola si cancella solo se da almeno Grace giri   *)
(*     non e' in nessun foglio e in nessuna spilla (l'eta' riparte quando   *)
(*     esce dall'ultimo foglio, non dalla nascita); una scrittura in volo   *)
(*     dura al massimo Hold <= Grace - 1 giri, un lettore senza la regola   *)
(*     del 404 tiene un foglio di al massimo RHold <= Grace giri; con la    *)
(*     regola "su 404 rileggi il foglio" il lettore puo' tenerlo quanto     *)
(*     vuole;                                                               *)
(*  4. cancellare un database lo fa uscire dalla famiglia: le sue scatole   *)
(*     restano finche' qualcuno le usa.                                     *)
(*                                                                          *)
(* Il tempo e' misurato in giri completati del raccoglitore (`cycle`).     *)
(*                                                                          *)
(* Controlli negativi (ognuno deve far trovare a TLC una violazione):       *)
(*  NoRegisterFirst  il fork scrive il foglio del ramo e si registra solo   *)
(*                   alla fine (registrazione dopo la condivisione)         *)
(*  SplitPin         il fork legge il foglio sorgente e scrive la spilla in *)
(*                   due passi separati (spilla non atomica)                *)
(*  OwnerDeletes     il lavaggio cancella subito le scatole sue che toglie  *)
(*                   dal foglio, se nessun foglio o spilla le usa in quel   *)
(*                   momento (piu' di un cancellatore)                      *)
(*  DropDeletesPrefix cancellare un database cancella tutte le sue scatole  *)
(*                   (come Admin::delete_db di SlateDB)                     *)
(*  NoRegCheck       il raccoglitore non ricontrolla il registro prima di   *)
(*                   cancellare                                             *)
(*  Grace = 0        nessuna eta' minima                                    *)
(*  AgeByCreation    eta' minima misurata dalla nascita della scatola, non  *)
(*                   da quando e' uscita dall'ultimo foglio                 *)
(*  CountSweeps      eta' minima come "vista inutilizzata in Grace giri di  *)
(*                   fila dal raccoglitore" (sembra giusta, non lo e')      *)
(*  Hold = Grace     una scrittura in volo dura un giro di troppo           *)
(*  RHold = Grace+1  un lettore tiene il foglio un giro di troppo           *)
(*  NoSlowCheck      il raccoglitore cancella anche se il suo giro e' durato*)
(*                   piu' di un giro (con due raccoglitori)                 *)
(***************************************************************************)
EXTENDS Naturals, FiniteSets

CONSTANTS DBs, Root, Collectors, Readers,
          MaxBoxes, MaxCycles, MaxPoints,
          Hold,     \* giri completi al massimo durante una scrittura in volo
          RHold,    \* eta' massima (in giri) del foglio tenuto da un lettore, senza la regola del 404
          Grace,    \* eta' minima in giri dall'ultima uscita da un foglio o da una spilla (0 = nessuna)
          Reread,
          NoRegisterFirst, SplitPin, OwnerDeletes, DropDeletesPrefix, NoRegCheck, AgeByCreation, CountSweeps, NoSlowCheck,
          Track   \* TRUE solo nei modelli di controllo: tiene la variabile fantasma `seen`

Boxes == 1..MaxBoxes
NoDB  == "nessuno"

VARIABLES
  boxSt,    \* scatola -> "none" (mai scritta) | "live" | "gone" (cancellata)
  owner,    \* scatola -> database proprietario (prefisso)
  bcyc,     \* scatola -> giro dell'ultimo "contatto": nascita, o uscita da un foglio o da una spilla
            \* (con AgeByCreation solo la nascita)
  nextBox,  \* prossima scatola libera
  mf,       \* database -> insieme di scatole del suo foglio ({} se il foglio non esiste)
  members,  \* registro: membri della famiglia
  pins,     \* registro: spille [k |-> "fork"|"point", db, id, pin]
  used,     \* nomi di database gia' usati (un nome non si riusa)
  nPoints,  \* punti di ripristino creati (limite del modello)
  fst,      \* database -> stato del fork che lo crea:
            \*   "none" | "read" | "pinned" | "written" | "done" | "aborted"
  fpin,     \* database -> scatole prese dalla sorgente
  wst,      \* database -> "idle" | "flush" | "compact" (scrittura in volo)
  wbox,     \* database -> scatola scritta e non ancora nel foglio
  win,      \* database -> scatole sostituite dalla compattazione in volo
  wcyc,     \* database -> giro in cui la scrittura in volo e' cominciata
  cst,      \* raccoglitore -> "idle" | "listed" | "reg"
  ccand,    \* raccoglitore -> candidati (scatole vive al LIST)
  cmem,     \* raccoglitore -> membri letti dal registro
  cpins,    \* raccoglitore -> spille lette dal registro
  cread,    \* raccoglitore -> membri di cui ha letto il foglio
  cused,    \* raccoglitore -> scatole viste nei fogli letti
  ccyc,     \* raccoglitore -> valore di `cycle` al suo LIST
  marked,   \* raccoglitore -> scatola -> giri consecutivi in cui l'ha vista inutilizzata (solo CountSweeps)
  cycle,    \* giri completati (tempo del modello)
  rst,      \* lettore -> "idle" | "hold"
  rdb,      \* lettore -> database letto
  rheld,    \* lettore -> scatole del foglio che tiene
  rcyc,     \* lettore -> giro in cui ha preso il foglio
  rfail,    \* TRUE appena un lettore riceve "scatola non trovata" e non puo' rimediare
  seen      \* fantasma, solo con Track: eventi rari gia' accaduti (controllo che il modello non sia vuoto)

vars == <<boxSt, owner, bcyc, nextBox, mf, members, pins, used, nPoints, fst, fpin,
          wst, wbox, win, wcyc, cst, ccand, cmem, cpins, cread, cused, ccyc, marked, cycle,
          rst, rdb, rheld, rcyc, rfail, seen>>
wvars == <<wst, wbox, win, wcyc>>
cvars == <<cst, ccand, cmem, cpins, cread, cused, ccyc, marked, cycle>>
rvars == <<rst, rdb, rheld, rcyc, rfail>>
Mark(ev) == IF Track THEN seen' = seen \cup {ev} ELSE UNCHANGED seen
fvars == <<fst, fpin, used>>

Live == {b \in Boxes : boxSt[b] = "live"}
\* le scatole in S escono da un foglio o da una spilla adesso: l'eta' minima riparte da qui
Touch(S) == IF AgeByCreation THEN UNCHANGED bcyc
            ELSE bcyc' = [b \in Boxes |-> IF b \in S THEN cycle ELSE bcyc[b]]
PinBoxes(P) == UNION {e.pin : e \in P}

TypeOK ==
  /\ boxSt \in [Boxes -> {"none", "live", "gone"}]
  /\ owner \in [Boxes -> DBs \cup {NoDB}]
  /\ bcyc \in [Boxes -> 0..MaxCycles]
  /\ nextBox \in 1..(MaxBoxes + 1)
  /\ mf \in [DBs -> SUBSET Boxes]
  /\ members \subseteq DBs
  /\ used \subseteq DBs
  /\ nPoints \in 0..MaxPoints
  /\ fst \in [DBs -> {"none", "read", "pinned", "written", "done", "aborted"}]
  /\ fpin \in [DBs -> SUBSET Boxes]
  /\ wst \in [DBs -> {"idle", "flush", "compact"}]
  /\ cst \in [Collectors -> {"idle", "listed", "reg"}]
  /\ cycle \in 0..MaxCycles
  /\ marked \in [Collectors -> [Boxes -> 0..MaxCycles]]
  /\ rst \in [Readers -> {"idle", "hold"}]
  /\ rfail \in BOOLEAN
  /\ seen \subseteq {"reread", "forkFromPoint", "reclaimDropped", "deleted"}

Init ==
  /\ boxSt = [b \in Boxes |-> IF b = 1 THEN "live" ELSE "none"]
  /\ owner = [b \in Boxes |-> IF b = 1 THEN Root ELSE NoDB]
  /\ bcyc = [b \in Boxes |-> 0]
  /\ nextBox = 2
  /\ mf = [d \in DBs |-> IF d = Root THEN {1} ELSE {}]
  /\ members = {Root}
  /\ pins = {}
  /\ used = {Root}
  /\ nPoints = 0
  /\ fst = [d \in DBs |-> "none"]
  /\ fpin = [d \in DBs |-> {}]
  /\ wst = [d \in DBs |-> "idle"]
  /\ wbox = [d \in DBs |-> 0]
  /\ win = [d \in DBs |-> {}]
  /\ wcyc = [d \in DBs |-> 0]
  /\ cst = [c \in Collectors |-> "idle"]
  /\ ccand = [c \in Collectors |-> {}]
  /\ cmem = [c \in Collectors |-> {}]
  /\ cpins = [c \in Collectors |-> {}]
  /\ cread = [c \in Collectors |-> {}]
  /\ cused = [c \in Collectors |-> {}]
  /\ ccyc = [c \in Collectors |-> 0]
  /\ marked = [c \in Collectors |-> [b \in Boxes |-> 0]]
  /\ cycle = 0
  /\ rst = [r \in Readers |-> "idle"]
  /\ rdb = [r \in Readers |-> Root]
  /\ rheld = [r \in Readers |-> {}]
  /\ rcyc = [r \in Readers |-> 0]
  /\ rfail = FALSE
  /\ seen = {}

-----------------------------------------------------------------------------
(* Scritture di un database: scarico (flush) e lavaggio (compattazione).   *)
(* Due passi: prima la scatola nuova su S3, poi il foglio (PUT condizionale*)
(* del solo scrittore del database).                                        *)

NewBox(d) ==
  /\ nextBox <= MaxBoxes
  /\ boxSt' = [boxSt EXCEPT ![nextBox] = "live"]
  /\ owner' = [owner EXCEPT ![nextBox] = d]
  /\ bcyc' = [bcyc EXCEPT ![nextBox] = cycle]
  /\ nextBox' = nextBox + 1
  /\ wbox' = [wbox EXCEPT ![d] = nextBox]
  /\ wcyc' = [wcyc EXCEPT ![d] = cycle]

FlushWrite(d) ==
  /\ d \in members /\ wst[d] = "idle"
  /\ NewBox(d)
  /\ wst' = [wst EXCEPT ![d] = "flush"]
  /\ win' = [win EXCEPT ![d] = {}]
  /\ UNCHANGED <<mf, members, pins, nPoints, fvars, cvars, rvars>>
  /\ UNCHANGED seen

\* qualsiasi compattazione di 1 o 2 scatole del foglio, anche ereditate (cosi' il ramo si "stacca")
CompactWrite(d) ==
  /\ d \in members /\ wst[d] = "idle"
  /\ \E S \in SUBSET mf[d] :
       /\ Cardinality(S) \in 1..2
       /\ NewBox(d)
       /\ wst' = [wst EXCEPT ![d] = "compact"]
       /\ win' = [win EXCEPT ![d] = S]
  /\ UNCHANGED <<mf, members, pins, nPoints, fvars, cvars, rvars>>
  /\ UNCHANGED seen

\* il foglio nuovo; valido solo se la scrittura e' durata al massimo Hold giri (ipotesi di tempo)
WriteCommit(d) ==
  /\ d \in members /\ wst[d] # "idle"
  /\ cycle - wcyc[d] <= Hold
  /\ mf' = [mf EXCEPT ![d] = (@ \ win[d]) \cup {wbox[d]}]
  /\ wst' = [wst EXCEPT ![d] = "idle"]
  /\ IF OwnerDeletes
       \* controllo negativo: il lavaggio cancella le scatole sue che toglie, se in questo momento
       \* nessun foglio e nessuna spilla le usa (vista atomica, piu' generosa del vero)
       THEN LET inUse == UNION {mf'[e] : e \in DBs} \cup PinBoxes(pins)
                del   == {b \in win[d] : owner[b] = d /\ b \notin inUse}
            IN boxSt' = [b \in Boxes |-> IF b \in del THEN "gone" ELSE boxSt[b]]
       ELSE UNCHANGED boxSt
  /\ Touch(win[d])
  /\ UNCHANGED <<owner, nextBox, wbox, win, wcyc, members, pins, nPoints, fvars, cvars, rvars>>
  /\ UNCHANGED seen

\* lo scrittore rinuncia (crash, tempo scaduto): la scatola resta orfana su S3
WriteAbort(d) ==
  /\ wst[d] # "idle"
  /\ wst' = [wst EXCEPT ![d] = "idle"]
  /\ UNCHANGED <<boxSt, owner, bcyc, nextBox, wbox, win, wcyc, mf, members, pins, nPoints, fvars, cvars, rvars>>
  /\ UNCHANGED seen

-----------------------------------------------------------------------------
(* Fork (ramo) e ripristino a un istante come ramo da un punto.             *)

\* sorgenti possibili: il foglio attuale di un membro, o la spilla di un punto di ripristino
Sources == {mf[s] : s \in members} \cup {e.pin : e \in {p \in pins : p.k = "point"}}

ForkEntry(dst, P) == [k |-> "fork", db |-> dst, id |-> 0, pin |-> P]

ForkStart(dst) ==
  /\ dst \notin used
  /\ \E P \in Sources :
       /\ IF P \notin {mf[s] : s \in members} THEN Mark("forkFromPoint") ELSE UNCHANGED seen
       /\ fpin' = [fpin EXCEPT ![dst] = P]
       /\ used' = used \cup {dst}
       /\ IF SplitPin \/ NoRegisterFirst
            THEN /\ fst' = [fst EXCEPT ![dst] = IF SplitPin THEN "read" ELSE "pinned"]
                 /\ UNCHANGED pins
            \* regola 2: la spilla e' scritta con la lettura della sorgente, in un solo passo atomico
            \* (spilla nel foglio della sorgente con PUT condizionale, come il checkpoint di SlateDB,
            \*  oppure sorgente ferma durante il fork, come fa oggi fork.rs)
            ELSE /\ fst' = [fst EXCEPT ![dst] = "pinned"]
                 /\ pins' = pins \cup {ForkEntry(dst, P)}
  /\ UNCHANGED <<boxSt, owner, bcyc, nextBox, mf, members, nPoints, wvars, cvars, rvars>>

\* solo con SplitPin: la spilla con le scatole lette prima, in un passo successivo
ForkRegister(dst) ==
  /\ fst[dst] = "read"
  /\ pins' = pins \cup {ForkEntry(dst, fpin[dst])}
  /\ fst' = [fst EXCEPT ![dst] = "pinned"]
  /\ UNCHANGED <<boxSt, owner, bcyc, nextBox, mf, members, nPoints, fpin, used, wvars, cvars, rvars>>
  /\ UNCHANGED seen

ForkWrite(dst) ==
  /\ fst[dst] = "pinned"
  /\ mf' = [mf EXCEPT ![dst] = fpin[dst]]
  /\ fst' = [fst EXCEPT ![dst] = "written"]
  /\ UNCHANGED <<boxSt, owner, bcyc, nextBox, members, pins, nPoints, fpin, used, wvars, cvars, rvars>>
  /\ UNCHANGED seen

\* un solo PUT del registro: il ramo diventa membro e la sua spilla di fork si toglie
ForkJoin(dst) ==
  /\ fst[dst] = "written"
  /\ members' = members \cup {dst}
  /\ pins' = {e \in pins : ~(e.k = "fork" /\ e.db = dst)}
  /\ fst' = [fst EXCEPT ![dst] = "done"]
  /\ UNCHANGED <<boxSt, owner, bcyc, nextBox, mf, nPoints, fpin, used, wvars, cvars, rvars>>
  /\ UNCHANGED seen

\* fork annullato (errore o crash): si cancella il foglio del ramo e poi la spilla
ForkAbort(dst) ==
  /\ fst[dst] \in {"read", "pinned", "written"}
  /\ mf' = [mf EXCEPT ![dst] = {}]
  /\ pins' = {e \in pins : ~(e.k = "fork" /\ e.db = dst)}
  /\ fst' = [fst EXCEPT ![dst] = "aborted"]
  /\ Touch(mf[dst] \cup fpin[dst])
  /\ UNCHANGED <<boxSt, owner, nextBox, members, nPoints, fpin, used, wvars, cvars, rvars>>
  /\ UNCHANGED seen

\* punto di ripristino: spilla del foglio attuale, atomica (PUT condizionale)
PointCreate(d) ==
  /\ d \in members /\ nPoints < MaxPoints
  /\ pins' = pins \cup {[k |-> "point", db |-> d, id |-> nPoints + 1, pin |-> mf[d]]}
  /\ nPoints' = nPoints + 1
  /\ UNCHANGED <<boxSt, owner, bcyc, nextBox, mf, members, fvars, wvars, cvars, rvars>>
  /\ UNCHANGED seen

PointExpire ==
  /\ \E e \in pins :
       /\ e.k = "point"
       /\ pins' = pins \ {e}
       /\ Touch(e.pin)
  /\ UNCHANGED <<boxSt, owner, nextBox, mf, members, nPoints, fvars, wvars, cvars, rvars>>
  /\ UNCHANGED seen

\* il database esce dalla famiglia (regola 4): via dal registro con i suoi punti, foglio cancellato
Drop(d) ==
  /\ d \in members
  /\ members' = members \ {d}
  /\ pins' = {e \in pins : ~(e.k = "point" /\ e.db = d)}
  /\ mf' = [mf EXCEPT ![d] = {}]
  /\ wst' = [wst EXCEPT ![d] = "idle"]
  /\ IF DropDeletesPrefix
       THEN boxSt' = [b \in Boxes |-> IF owner[b] = d /\ boxSt[b] = "live" THEN "gone" ELSE boxSt[b]]
       ELSE UNCHANGED boxSt
  /\ Touch(mf[d] \cup PinBoxes({e \in pins : e.k = "point" /\ e.db = d}))
  /\ UNCHANGED <<owner, nextBox, wbox, win, wcyc, nPoints, fvars, cvars, rvars>>
  /\ UNCHANGED seen

-----------------------------------------------------------------------------
(* Il raccoglitore della famiglia, in piu' passi non atomici:               *)
(*  1. LIST delle scatole (candidati);                                      *)
(*  2. lettura del registro (membri e spille);                              *)
(*  3. lettura del foglio di ogni membro, uno alla volta;                   *)
(*  4. nuova lettura del registro: se e' cambiato, o se dal LIST e' gia'   *)
(*     passato un giro intero, rinuncia al giro;                            *)
(*     altrimenti cancella i candidati che nessun foglio letto e nessuna    *)
(*     spilla usa e che hanno l'eta' minima.                                *)

CList(c) ==
  /\ cst[c] = "idle" /\ cycle < MaxCycles
  /\ cst' = [cst EXCEPT ![c] = "listed"]
  /\ ccand' = [ccand EXCEPT ![c] = Live]
  /\ ccyc' = [ccyc EXCEPT ![c] = cycle]
  /\ UNCHANGED <<boxSt, owner, bcyc, nextBox, mf, members, pins, nPoints, fvars, wvars,
                 cmem, cpins, cread, cused, marked, cycle, rvars>>
  /\ UNCHANGED seen

CReadReg(c) ==
  /\ cst[c] = "listed"
  /\ cst' = [cst EXCEPT ![c] = "reg"]
  /\ cmem' = [cmem EXCEPT ![c] = members]
  /\ cpins' = [cpins EXCEPT ![c] = pins]
  /\ cread' = [cread EXCEPT ![c] = {}]
  /\ cused' = [cused EXCEPT ![c] = {}]
  /\ UNCHANGED <<boxSt, owner, bcyc, nextBox, mf, members, pins, nPoints, fvars, wvars,
                 ccand, ccyc, marked, cycle, rvars>>
  /\ UNCHANGED seen

CReadMf(c) ==
  /\ cst[c] = "reg"
  /\ \E d \in cmem[c] \ cread[c] :
       /\ cread' = [cread EXCEPT ![c] = @ \cup {d}]
       /\ cused' = [cused EXCEPT ![c] = @ \cup mf[d]]
  /\ UNCHANGED <<boxSt, owner, bcyc, nextBox, mf, members, pins, nPoints, fvars, wvars,
                 cst, ccand, cmem, cpins, ccyc, marked, cycle, rvars>>
  /\ UNCHANGED seen

CAbort(c) ==
  /\ cst[c] = "reg" /\ cread[c] = cmem[c]
  /\ \/ ~NoRegCheck /\ (members # cmem[c] \/ pins # cpins[c])
     \* giro troppo lento: dal suo LIST e' passato un giro intero (di un altro raccoglitore)
     \/ ~NoSlowCheck /\ cycle # ccyc[c]
  /\ cst' = [cst EXCEPT ![c] = "idle"]
  /\ UNCHANGED <<boxSt, owner, bcyc, nextBox, mf, members, pins, nPoints, fvars, wvars,
                 ccand, cmem, cpins, cread, cused, ccyc, marked, cycle, rvars>>
  /\ UNCHANGED seen

CSweep(c) ==
  /\ cst[c] = "reg" /\ cread[c] = cmem[c]
  /\ cycle < MaxCycles   \* limite del modello (con due raccoglitori serve anche qui)
  /\ (NoRegCheck \/ (members = cmem[c] /\ pins = cpins[c]))
  /\ (NoSlowCheck \/ cycle = ccyc[c])
  /\ LET unused == ccand[c] \ (cused[c] \cup PinBoxes(cpins[c]))
         del    == {b \in unused : IF CountSweeps THEN marked[c][b] + 1 >= Grace
                                    ELSE cycle - bcyc[b] >= Grace}
     IN /\ boxSt' = [b \in Boxes |-> IF b \in del THEN "gone" ELSE boxSt[b]]
        /\ IF CountSweeps
             THEN marked' = [marked EXCEPT ![c] = [b \in Boxes |-> IF b \in unused \ del THEN marked[c][b] + 1 ELSE 0]]
             ELSE UNCHANGED marked
        /\ IF Track THEN seen' = seen \cup (IF del # {} THEN {"deleted"} ELSE {})
                                     \cup (IF \E b \in del : owner[b] \in used \ members
                                                           /\ owner[b] # NoDB /\ fst[owner[b]] # "aborted"
                                           THEN {"reclaimDropped"} ELSE {})
           ELSE UNCHANGED seen
  /\ cst' = [cst EXCEPT ![c] = "idle"]
  /\ cycle' = cycle + 1
  /\ UNCHANGED <<owner, bcyc, nextBox, mf, members, pins, nPoints, fvars, wvars,
                 ccand, cmem, cpins, cread, cused, ccyc, rvars>>

-----------------------------------------------------------------------------
(* Lettori (camerieri) che tengono un foglio anche vecchio.                 *)

RTake(r) ==
  /\ rst[r] = "idle"
  /\ \E d \in members :
       /\ rst' = [rst EXCEPT ![r] = "hold"]
       /\ rdb' = [rdb EXCEPT ![r] = d]
       /\ rheld' = [rheld EXCEPT ![r] = mf[d]]
       /\ rcyc' = [rcyc EXCEPT ![r] = cycle]
  /\ UNCHANGED <<boxSt, owner, bcyc, nextBox, mf, members, pins, nPoints, fvars, wvars, cvars, rfail>>
  /\ UNCHANGED seen

\* lettura con il foglio tenuto. Senza la regola del 404 vale l'ipotesi di tempo: il foglio
\* tenuto ha al massimo RHold giri (piu' vecchio: il lettore lo rilegge prima, con RTake).
RRead(r) ==
  /\ rst[r] = "hold"
  /\ Reread \/ cycle - rcyc[r] <= RHold
  /\ IF rheld[r] \subseteq Live
       THEN /\ rst' = [rst EXCEPT ![r] = "idle"]
            /\ UNCHANGED <<rheld, rcyc, rfail, seen>>
       ELSE IF Reread
         \* regola "su 404 rileggi il foglio"
         THEN IF rdb[r] \in members
                THEN /\ rheld' = [rheld EXCEPT ![r] = mf[rdb[r]]]
                     /\ rcyc' = [rcyc EXCEPT ![r] = cycle]
                     /\ Mark("reread")
                     /\ UNCHANGED <<rst, rfail>>
                ELSE /\ rst' = [rst EXCEPT ![r] = "idle"]   \* database cancellato: risposta corretta
                     /\ UNCHANGED <<rheld, rcyc, rfail, seen>>
         ELSE IF rdb[r] \in members
                \* "scatola non trovata" su un database che esiste ancora: errore per il client
                THEN /\ rfail' = TRUE
                     /\ UNCHANGED <<rst, rheld, rcyc, seen>>
                \* database cancellato nel frattempo: l'errore e' la risposta giusta
                ELSE /\ rst' = [rst EXCEPT ![r] = "idle"]
                     /\ UNCHANGED <<rheld, rcyc, rfail, seen>>
  /\ UNCHANGED <<boxSt, owner, bcyc, nextBox, mf, members, pins, nPoints, fvars, wvars, cvars, rdb>>

RLeave(r) ==
  /\ rst[r] = "hold"
  /\ rst' = [rst EXCEPT ![r] = "idle"]
  /\ UNCHANGED <<boxSt, owner, bcyc, nextBox, mf, members, pins, nPoints, fvars, wvars, cvars,
                 rdb, rheld, rcyc, rfail>>
  /\ UNCHANGED seen

-----------------------------------------------------------------------------

Next ==
  \/ \E d \in DBs : \/ FlushWrite(d) \/ CompactWrite(d) \/ WriteCommit(d) \/ WriteAbort(d)
                   \/ ForkStart(d) \/ ForkRegister(d) \/ ForkWrite(d) \/ ForkJoin(d) \/ ForkAbort(d)
                   \/ PointCreate(d) \/ Drop(d)
  \/ PointExpire
  \/ \E c \in Collectors : CList(c) \/ CReadReg(c) \/ CReadMf(c) \/ CAbort(c) \/ CSweep(c)
  \/ \E r \in Readers : RTake(r) \/ RRead(r) \/ RLeave(r)

Spec == Init /\ [][Next]_vars

(* Vista per TLC (VIEW): due stati che differiscono solo in valori che non verranno mai piu' letti *)
(* (campi di un'operazione finita, che la prossima sovrascrive prima di leggerli) sono lo stesso   *)
(* stato. Riduce lo spazio senza togliere comportamenti.                                          *)
View == << boxSt, owner, bcyc, nextBox, mf, members, pins, used, nPoints, fst,
           [d \in DBs |-> IF fst[d] \in {"read", "pinned"} THEN fpin[d] ELSE {}],
           [d \in DBs |-> IF wst[d] = "idle" THEN <<"idle">> ELSE <<wst[d], wbox[d], win[d], wcyc[d]>>],
           [c \in Collectors |-> IF cst[c] = "idle" THEN <<"idle">>
                                 ELSE <<cst[c], ccand[c], cmem[c], cpins[c], cread[c], cused[c], ccyc[c]>>],
           IF CountSweeps THEN marked ELSE 0, cycle,
           [r \in Readers |-> IF rst[r] = "idle" THEN <<"idle">> ELSE <<rdb[r], rheld[r], rcyc[r]>>],
           rfail, seen >>

-----------------------------------------------------------------------------
(* Invarianti                                                               *)

\* nessun foglio vivo (membro, o ramo scritto e non ancora membro) punta a una scatola cancellata
NoDangling == \A d \in DBs : (d \in members \/ fst[d] = "written") => mf[d] \subseteq Live

\* ogni spilla (fork in corso, punto di ripristino) e' ancora utilizzabile
PinsLive == PinBoxes(pins) \subseteq Live

\* nessun lettore riceve "scatola non trovata" senza rimedio su un database che esiste ancora
ReadersOK == ~rfail

\* una scrittura in volo non pubblica mai un foglio con una scatola gia' cancellata:
\* coperto da NoDangling dopo WriteCommit; qui il controllo prima del commit
InFlightLive == \A d \in members : wst[d] # "idle" /\ cycle - wcyc[d] <= Hold => wbox[d] \in Live

Safety == NoDangling /\ PinsLive /\ ReadersOK

(* Invarianti "di controllo del modello": devono essere VIOLATE, per mostrare che *)
(* gli scenari interessanti sono raggiungibili (il modello non e' vuoto).        *)

\* il raccoglitore non cancella mai nulla
SanityNoDelete == "deleted" \notin seen
\* mai: un ramo vivo che legge scatole di un padre uscito dalla famiglia
SanityNoOrphanParent ==
  ~ \E d \in members, b \in Boxes : b \in mf[d] /\ owner[b] # d /\ owner[b] \notin members
\* mai: il raccoglitore cancella una scatola di un database uscito dalla famiglia
SanityNoReclaimDropped == "reclaimDropped" \notin seen
\* mai: un lettore rilegge il foglio dopo un 404
SanityNoReread == "reread" \notin seen
\* mai: un ramo nato da un punto di ripristino
SanityNoForkFromPoint == "forkFromPoint" \notin seen
\* mai: un nipote (ramo di un ramo) vivo che legge ancora una scatola del nonno uscito dalla famiglia
SanityNoGrandchild ==
  ~ \E d \in members, b \in Boxes :
      /\ b \in mf[d] /\ owner[b] = Root /\ Root \notin members
      /\ \E m \in DBs : m # Root /\ m # d /\ m \notin members /\ fst[m] = "done"
=============================================================================
