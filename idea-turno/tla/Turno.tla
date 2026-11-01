------------------------------- MODULE Turno -------------------------------
(***************************************************************************)
(* La regola del turno del "quaderno a turno" (idea B, corretta dal        *)
(* revisore: punti A, B, C, più il numero di turno nel manifest).          *)
(*                                                                         *)
(* Un solo database d. Il quaderno è un file system condiviso (EFS) che    *)
(* serializza le operazioni sui file. Ogni cameriere scrive il suo libro   *)
(* (libri/<c>/<epoca>.log). Il turno di d si prende creando turni/d/<n>    *)
(* con O_EXCL; l'epoca di un cameriere si cambia creando epoche/<c>/<e>.   *)
(*                                                                         *)
(* Le tre azioni del protocollo:                                           *)
(*   1. Scrivi + Fsync: il titolare scrive un lotto nel libro e fa fsync.  *)
(*   2. Controlla: DOPO l'fsync, una domanda vera al server sulla propria  *)
(*      epoca; se è ancora la sua, conferma il lotto.                      *)
(*   3. Prendi il turno: PRIMA turni/d/<n> (nome solo n), POI una epoca    *)
(*      nuova per ogni titolare precedente della catena, POI la lettura    *)
(*      dei loro libri.                                                    *)
(* Più due azioni di contorno: la reazione di chi vede un'epoca nuova      *)
(* (rilegge i turni dal server) e lo scarico su S3 recintato dal numero    *)
(* di turno scritto nel manifest.                                          *)
(*                                                                         *)
(* Le costanti booleane accendono le regole. Con tutte TRUE il modello è   *)
(* quello corretto; i file Turno_neg_*.cfg ne spengono una alla volta e    *)
(* TLC deve trovare la violazione.                                         *)
(***************************************************************************)
EXTENDS Naturals, FiniteSets

CONSTANTS
    Camerieri,            \* i camerieri (processi su istanze diverse)
    Primo,                \* il titolare iniziale di d (turno 1)
    MaxRec,               \* quante scritture al massimo (record 1..MaxRec)
    MaxTurno,             \* turni al massimo (prese = MaxTurno - 1)
    MaxEpoca,             \* limite per tenere finito lo spazio degli stati
    NomeUnico,            \* A: il file di turno si chiama solo <n>
    TurnoPrimaDiEpoca,    \* B: prima il file di turno, poi l'epoca
    ControlloVero,        \* C: il controllo chiede al server (niente cache)
    ControlloDopoFsync,   \* C: il controllo si fa DOPO l'fsync
    RecintaLaCatena,      \* nuova epoca a TUTTI i titolari precedenti
    RecintoManifest,      \* numero di turno nel manifest
    RiletturaDopoEpoche   \* dopo le epoche, rilegge i turni: se c'è un turno più
                          \* alto del suo, rinuncia (trovato da TLC con 4 camerieri)

VARIABLES
    turni,        \* file di turno sul server: insieme di <<n, cameriere>>
    epoche,       \* epoche[c]: numero più alto tra i file epoche/c/*
    libri,        \* record resi durevoli: insieme di <<c, epoca, r>>
    pc,           \* "normale", "reagisci", "reagisci2", "crea", "epoche", "verifica", "turno", "leggi"
    mioTurno,     \* il turno di d che c crede di avere (0 = nessuno)
    miaEpoca,     \* l'epoca del libro che c sta scrivendo
    lotto,        \* record scritti nel libro ma non ancora fsync
    inAttesa,     \* record fsync, non ancora confermati
    okPrima,      \* (variante sbagliata) controllo fatto prima dell'fsync
    conosce,      \* record che c conosce (suoi o letti alla presa)
    eredita,      \* record letti alla presa, scritti nel libro come "presa"
    cache,        \* (variante sbagliata) copia locale dell'epoca
    presaN,       \* il numero di turno della presa in corso
    catena,       \* catena[h][c]: fin dove h legge i libri di c (epoca che h ha
                  \* creato per c, esclusa); 0 = c non è nella catena
    epocaVista,   \* l'epoca letta nel primo passo della reazione
    confermati,   \* record confermati al cliente
    prossimo,     \* il prossimo record da scrivere
    manifest,     \* [turno |-> n, rec |-> insieme di record]
    maiScaricati  \* tutti i record mai entrati nel manifest

vars == <<turni, epoche, libri, pc, mioTurno, miaEpoca, lotto, inAttesa,
          okPrima, conosce, eredita, cache, presaN, catena, epocaVista,
          confermati, prossimo, manifest, maiScaricati>>

Max(S) == IF S = {} THEN 0 ELSE CHOOSE x \in S : \A y \in S : x >= y
NumeriTurno == {t[1] : t \in turni}
TurnoMax == Max(NumeriTurno)
Titolari(m) == {t[2] : t \in {u \in turni : u[1] < m}}
Ultimi(m) == {t[2] : t \in {u \in turni : u[1] = m - 1}}

Init ==
    /\ turni = {<<1, Primo>>}
    /\ epoche = [c \in Camerieri |-> 1]
    /\ libri = {}
    /\ pc = [c \in Camerieri |-> "normale"]
    /\ mioTurno = [c \in Camerieri |-> IF c = Primo THEN 1 ELSE 0]
    /\ miaEpoca = [c \in Camerieri |-> 1]
    /\ lotto = [c \in Camerieri |-> {}]
    /\ inAttesa = [c \in Camerieri |-> {}]
    /\ okPrima = [c \in Camerieri |-> FALSE]
    /\ conosce = [c \in Camerieri |-> {}]
    /\ eredita = [c \in Camerieri |-> {}]
    /\ cache = [c \in Camerieri |-> 1]
    /\ presaN = [c \in Camerieri |-> 0]
    /\ catena = [c \in Camerieri |-> [x \in Camerieri |-> 0]]
    /\ epocaVista = [c \in Camerieri |-> 0]
    /\ confermati = {}
    /\ prossimo = 1
    /\ manifest = [turno |-> 0, rec |-> {}]
    /\ maiScaricati = {}

Titolare(c) == pc[c] = "normale" /\ mioTurno[c] > 0

(* ---------- Azione 1: scrivere un lotto e farne l'fsync ---------- *)

Scrivi(c) ==
    /\ Titolare(c)
    /\ prossimo <= MaxRec
    /\ lotto' = [lotto EXCEPT ![c] = @ \cup {prossimo}]
    /\ conosce' = [conosce EXCEPT ![c] = @ \cup {prossimo}]
    /\ prossimo' = prossimo + 1
    /\ UNCHANGED <<turni, epoche, libri, pc, mioTurno, miaEpoca, inAttesa,
                   okPrima, eredita, cache, presaN, catena, epocaVista,
                   confermati, manifest, maiScaricati>>

\* L'fsync rende il lotto durevole sul server (multi-zona per EFS).
\* Nella variante sbagliata (controllo prima dell'fsync) si conferma qui,
\* fidandosi di un controllo fatto prima.
Fsync(c) ==
    /\ Titolare(c)
    /\ lotto[c] # {}
    /\ libri' = libri \cup {<<c, miaEpoca[c], r>> : r \in lotto[c]}
    /\ IF ControlloDopoFsync \/ ~okPrima[c]
         THEN /\ inAttesa' = [inAttesa EXCEPT ![c] = @ \cup lotto[c]]
              /\ UNCHANGED confermati
         ELSE /\ confermati' = confermati \cup lotto[c]
              /\ UNCHANGED inAttesa
    /\ lotto' = [lotto EXCEPT ![c] = {}]
    /\ okPrima' = [okPrima EXCEPT ![c] = FALSE]
    /\ UNCHANGED <<turni, epoche, pc, mioTurno, miaEpoca, conosce, eredita,
                   cache, presaN, catena, epocaVista, prossimo, manifest,
                   maiScaricati>>

(* ---------- Azione 2: il controllo del turno e la conferma ---------- *)

EpocaLetta(c) == IF ControlloVero THEN epoche[c] ELSE cache[c]

\* Regola corretta: dopo l'fsync, una domanda vera al server.
Controlla(c) ==
    /\ ControlloDopoFsync
    /\ Titolare(c)
    /\ inAttesa[c] # {}
    /\ IF EpocaLetta(c) = miaEpoca[c]
         THEN /\ confermati' = confermati \cup inAttesa[c]
              /\ UNCHANGED <<pc, lotto>>
         ELSE /\ pc' = [pc EXCEPT ![c] = "reagisci"]   \* smette di confermare
              /\ lotto' = [lotto EXCEPT ![c] = {}]       \* lotti ambigui: il
              /\ UNCHANGED confermati                    \* cliente ritenta
    /\ inAttesa' = [inAttesa EXCEPT ![c] = {}]
    /\ UNCHANGED <<turni, epoche, libri, mioTurno, miaEpoca, okPrima, conosce,
                   eredita, cache, presaN, catena, epocaVista, prossimo,
                   manifest, maiScaricati>>

\* Variante sbagliata: il controllo prima dell'fsync.
ControllaPrima(c) ==
    /\ ~ControlloDopoFsync
    /\ Titolare(c)
    /\ lotto[c] # {}
    /\ ~okPrima[c]
    /\ IF EpocaLetta(c) = miaEpoca[c]
         THEN /\ okPrima' = [okPrima EXCEPT ![c] = TRUE]
              /\ UNCHANGED <<pc, lotto, inAttesa>>
         ELSE /\ pc' = [pc EXCEPT ![c] = "reagisci"]
              /\ lotto' = [lotto EXCEPT ![c] = {}]
              /\ inAttesa' = [inAttesa EXCEPT ![c] = {}]
              /\ UNCHANGED okPrima
    /\ UNCHANGED <<turni, epoche, libri, mioTurno, miaEpoca, conosce, eredita,
                   cache, presaN, catena, epocaVista, confermati, prossimo,
                   manifest, maiScaricati>>

\* La cache degli attributi NFS si aggiorna quando vuole lei.
AggiornaCache(c) ==
    /\ ~ControlloVero
    /\ cache[c] # epoche[c]
    /\ cache' = [cache EXCEPT ![c] = epoche[c]]
    /\ UNCHANGED <<turni, epoche, libri, pc, mioTurno, miaEpoca, lotto,
                   inAttesa, okPrima, conosce, eredita, presaN, catena,
                   epocaVista, confermati, prossimo, manifest, maiScaricati>>

(* ---------- La reazione: chi vede un'epoca nuova ---------- *)

\* Passo 1: legge l'epoca dal server.
Reagisci1(c) ==
    /\ pc[c] = "reagisci"
    /\ epocaVista' = [epocaVista EXCEPT ![c] = epoche[c]]
    /\ pc' = [pc EXCEPT ![c] = "reagisci2"]
    /\ UNCHANGED <<turni, epoche, libri, mioTurno, miaEpoca, lotto, inAttesa,
                   okPrima, conosce, eredita, cache, presaN, catena,
                   confermati, prossimo, manifest, maiScaricati>>

\* Passo 2: rilegge i turni dal server; se c'è un turno più alto del suo,
\* lascia d. Poi apre il libro dell'epoca letta al passo 1.
Reagisci2(c) ==
    /\ pc[c] = "reagisci2"
    /\ IF \E t \in turni : t[1] > mioTurno[c]
         THEN mioTurno' = [mioTurno EXCEPT ![c] = 0]
         ELSE UNCHANGED mioTurno
    /\ miaEpoca' = [miaEpoca EXCEPT ![c] = epocaVista[c]]
    /\ pc' = [pc EXCEPT ![c] = "normale"]
    /\ UNCHANGED <<turni, epoche, libri, lotto, inAttesa, okPrima, conosce,
                   eredita, cache, presaN, catena, epocaVista, confermati,
                   prossimo, manifest, maiScaricati>>

(* ---------- Azione 3: prendere il turno ---------- *)

PuoCreare(n, h) ==
    IF NomeUnico THEN \A t \in turni : t[1] # n      \* turni/d/<n>
                 ELSE <<n, h>> \notin turni            \* turni/d/<n>-<h>

\* Passo 1: legge i turni dal server e sceglie n = più alto + 1.
Inizia(h) ==
    /\ pc[h] = "normale"
    /\ mioTurno[h] = 0
    /\ TurnoMax + 1 <= MaxTurno
    /\ presaN' = [presaN EXCEPT ![h] = TurnoMax + 1]
    /\ pc' = [pc EXCEPT ![h] = IF TurnoPrimaDiEpoca THEN "crea" ELSE "epoche"]
    /\ UNCHANGED <<turni, epoche, libri, mioTurno, miaEpoca, lotto, inAttesa,
                   okPrima, conosce, eredita, cache, catena, epocaVista,
                   confermati, prossimo, manifest, maiScaricati>>

\* Passo 1b (regola corretta): crea il file di turno con O_EXCL, subito.
\* Tra la lettura e la creazione un altro può arrivare prima: se O_EXCL
\* fallisce si rinuncia e si rilegge.
CreaTurno(h) ==
    /\ pc[h] = "crea"
    /\ IF PuoCreare(presaN[h], h)
         THEN /\ turni' = turni \cup {<<presaN[h], h>>}
              /\ pc' = [pc EXCEPT ![h] = "epoche"]
         ELSE /\ pc' = [pc EXCEPT ![h] = "normale"]
              /\ UNCHANGED turni
    /\ UNCHANGED <<epoche, libri, mioTurno, miaEpoca, lotto, inAttesa, okPrima,
                   conosce, eredita, cache, presaN, catena, epocaVista,
                   confermati, prossimo, manifest, maiScaricati>>

\* Passo 2: una epoca nuova (max + 1, O_EXCL) per ogni titolare precedente.
\* Variante sbagliata: solo per il titolare del turno n - 1.
Epoche(h) ==
    /\ pc[h] = "epoche"
    /\ LET n == presaN[h]
           tutti == Titolari(n) \ {h}
           daRecintare == IF RecintaLaCatena THEN tutti ELSE Ultimi(n) \ {h}
       IN /\ catena' = [catena EXCEPT ![h] =
                [c \in Camerieri |->
                   IF c \notin tutti THEN 0
                   ELSE IF c \in daRecintare THEN epoche[c] + 1
                   ELSE MaxEpoca + 1]]   \* non recintato: legge tutto
          /\ epoche' = [c \in Camerieri |->
                          IF c \in daRecintare THEN epoche[c] + 1
                                               ELSE epoche[c]]
    /\ pc' = [pc EXCEPT ![h] = IF ~TurnoPrimaDiEpoca THEN "turno"
                               ELSE IF RiletturaDopoEpoche THEN "verifica"
                               ELSE "leggi"]
    /\ UNCHANGED <<turni, libri, mioTurno, miaEpoca, lotto, inAttesa, okPrima,
                   conosce, eredita, cache, presaN, epocaVista, confermati,
                   prossimo, manifest, maiScaricati>>

\* Passo 2b: rilegge i turni dal server. Se nel frattempo qualcuno ha preso un
\* turno più alto, rinuncia: altrimenti un subentrante lento, recintando tardi,
\* leggerebbe (entro i suoi limiti di epoca) lotti scritti da un titolare più
\* nuovo e potrebbe scaricarli su S3 con il suo turno vecchio.
Verifica(h) ==
    /\ pc[h] = "verifica"
    /\ pc' = [pc EXCEPT ![h] = IF \E t \in turni : t[1] > presaN[h]
                               THEN "normale" ELSE "leggi"]
    /\ UNCHANGED <<turni, epoche, libri, mioTurno, miaEpoca, lotto, inAttesa,
                   okPrima, conosce, eredita, cache, presaN, catena, epocaVista,
                   confermati, prossimo, manifest, maiScaricati>>

\* Solo nella variante sbagliata (prima l'epoca): il file di turno dopo.
TurnoDopo(h) ==
    /\ pc[h] = "turno"
    /\ IF PuoCreare(presaN[h], h)
         THEN /\ turni' = turni \cup {<<presaN[h], h>>}
              /\ pc' = [pc EXCEPT ![h] = "leggi"]
         ELSE /\ pc' = [pc EXCEPT ![h] = "normale"]    \* ha perso: rinuncia
              /\ UNCHANGED turni
    /\ UNCHANGED <<epoche, libri, mioTurno, miaEpoca, lotto, inAttesa, okPrima,
                   conosce, eredita, cache, presaN, catena, epocaVista,
                   confermati, prossimo, manifest, maiScaricati>>

\* Passo 3: legge i libri dei titolari precedenti (e le loro "prese") e
\* comincia a servire d. Non cambia la propria epoca: la cambia solo la
\* reazione, che rilegge i turni.
Leggi(h) ==
    /\ pc[h] = "leggi"
    /\ LET lim == catena[h]
           letti == {t[3] : t \in {u \in libri : u[2] < lim[u[1]]}}
                    \cup UNION {eredita[c] : c \in {x \in Camerieri : lim[x] > 0}}
       IN /\ conosce' = [conosce EXCEPT ![h] = @ \cup letti]
          /\ eredita' = [eredita EXCEPT ![h] = letti]
    /\ mioTurno' = [mioTurno EXCEPT ![h] = presaN[h]]
    /\ pc' = [pc EXCEPT ![h] = "normale"]
    /\ UNCHANGED <<turni, epoche, libri, miaEpoca, lotto, inAttesa, okPrima,
                   cache, presaN, catena, epocaVista, confermati, prossimo,
                   manifest, maiScaricati>>

(* ---------- Lo scarico su S3 (manifest con If-Match) ---------- *)

\* Si scaricano solo record confermati. Con la recinzione il manifest porta
\* il numero di turno e rifiuta chi ha un turno più basso.
Scarica(c) ==
    /\ Titolare(c)
    /\ ~RecintoManifest \/ manifest.turno <= mioTurno[c]
    /\ LET contenuto == conosce[c] \cap confermati IN
         /\ manifest' = [turno |-> mioTurno[c], rec |-> contenuto]
         /\ maiScaricati' = maiScaricati \cup contenuto
    /\ UNCHANGED <<turni, epoche, libri, pc, mioTurno, miaEpoca, lotto,
                   inAttesa, okPrima, conosce, eredita, cache, presaN, catena,
                   epocaVista, confermati, prossimo>>

Next ==
    \E c \in Camerieri :
        \/ Scrivi(c) \/ Fsync(c) \/ Controlla(c) \/ ControllaPrima(c)
        \/ AggiornaCache(c) \/ Reagisci1(c) \/ Reagisci2(c)
        \/ Inizia(c) \/ CreaTurno(c) \/ Epoche(c) \/ Verifica(c) \/ TurnoDopo(c) \/ Leggi(c)
        \/ Scarica(c)

Spec == Init /\ [][Next]_vars

(* ---------- Le proprietà ---------- *)

\* Banco 9: per ogni numero di turno, un solo vincitore.
UnSoloVincitore ==
    \A x, y \in turni : x[1] = y[1] => x[2] = y[2]

\* L'invariante chiesta: nessuna conferma che il nuovo titolare non legge.
\* Chi ha finito la presa e ha il turno più alto conosce ogni record
\* confermato (anche quelli confermati da un vecchio titolare zombie).
NessunaConfermaPersa ==
    \A h \in Camerieri :
        (Titolare(h) /\ mioTurno[h] = TurnoMax) => confermati \subseteq conosce[h]

\* Il manifest non perde mai un record che ci era entrato.
ManifestNonPerde == maiScaricati \subseteq manifest.rec

\* Solo record confermati finiscono su S3.
SoloConfermatiSuS3 == manifest.rec \subseteq confermati

Limiti == \A c \in Camerieri : epoche[c] <= MaxEpoca

TypeOK ==
    /\ turni \subseteq (1..MaxTurno) \X Camerieri
    /\ \A c \in Camerieri : mioTurno[c] \in 0..MaxTurno
    /\ confermati \subseteq 1..MaxRec
=============================================================================
