# Lavaggio per cliente: cosa verifichiamo (scritto prima di far girare TLC)

Sessione M28, 2026-11-03. Solo carta, $0. Specifica: `LavaggioPerCliente.tla` in questa cartella.

Scopo: sapere, prima di scrivere il codice del pezzo 1 di `PLAN-LAVAGGIO.md`, se lo scambio delle
scatole (SST) nel foglio indice (manifest) resta corretto mentre girano insieme scritture, scarico
(flush), lavaggio (compaction) per cliente, lettori con istantanea (snapshot), rami (fork),
cancellazione di un cliente (GDPR), pulizia (GC) e crash del lavaggio a metà.

La prova non sceglie una strategia di lavaggio: il lavaggio del modello può prendere **qualsiasi**
gruppo di almeno 2 scatole dove sta un cliente sopra il tetto, anche non vicine in età. Quindi vale
per i "livelli geometrici", per il tetto a 2–6 pile e per qualsiasi via di mezzo scelga M26.

## Come è fatto il modello (in parole)

- **Il quaderno (log)** dà a ogni scrittura una posizione 1, 2, 3… Una scrittura cambia il documento
  di un cliente in una collezione (nuovo valore, oppure cancellato) e, nello **stesso record**, le sue
  voci di indice: spegne quella del valore vecchio e accende quella del valore nuovo (come fa oggi il
  motore con un solo batch).
- **La cancellazione di un cliente** è anch'essa un record del quaderno, alla posizione E: "il cliente
  X è cancellato fino a E". Da quel momento chi legge non vede più niente di X scritto prima di E.
  Quando lo scarico porta quel record nel foglio indice, diventa una riga della **lista dei
  cancellati** (ledger) del foglio. Poi la **riscrittura mirata** rifà solo le scatole dove X ha
  ancora dati vecchi, e solo alla fine la riga si toglie dalla lista.
- **Lo scarico (flush)** prende la coda del quaderno e la sigilla in una scatola nuova (mescolata,
  tutti i clienti), poi aggiorna il foglio: scatole, posizione scaricata, lista dei cancellati.
- **Il lavaggio** sceglie un cliente con più pile del tetto, legge alcune sue scatole, tiene per ogni
  chiave solo la versione più nuova, toglie le lapidi (tombstone) solo se sotto non c'è niente di più
  vecchio, toglie i dati coperti dalla lista dei cancellati, scrive **una scatola per cliente**, una
  alla volta (si può fermare a metà), poi scambia il foglio con **una sola scrittura condizionale**
  che riesce solo se tutte le sue scatole di partenza sono ancora nel foglio.
- **I lettori** prendono un'istantanea: foglio + posizione S del quaderno. Poi, in due momenti
  diversi, leggono l'indice e il documento. Leggono dalle scatole del foglio preso e dalla coda del
  quaderno fino a S.
- **Il ramo (fork)** usa la regola già decisa (DECISIONS 2026-11-02, "la spilla del fork con un tetto
  di tempo"): legge il foglio e la coda del quaderno, poi mette la spilla entro FHold giri, altrimenti
  ricomincia. Il ramo eredita anche la lista dei cancellati.
- **La pulizia (GC)** cancella una scatola solo se non è nel foglio, non è nella spilla di un ramo e
  ha l'età minima (Grace giri da quando è nata o è uscita dall'ultimo foglio o spilla). Ogni giro di
  pulizia fa invecchiare di 1 tutto (scatole, lettori, scrittori in volo). Chi scrive o legge ha un
  tetto di tempo: scrittori Hold giri, lettori RHold giri, fork FHold giri, tutti minori di Grace
  (sono le regole verificate in tla-rami).
- **Il crash**: il lavaggio, lo scarico o la riscrittura mirata si possono fermare in qualsiasi
  momento (al massimo MaxCrash volte per esecuzione, così il modello finisce). Le scatole già scritte
  restano lì, orfane. Il riavvio è un lavaggio nuovo da capo.

Le età sono contatori che si fermano al tetto (età "sature"): così il modello è finito anche senza un
numero massimo di giri, e TLC può verificare le proprietà di vivezza ("prima o poi succede").

## Le proprietà

Invarianti (devono valere in ogni stato raggiungibile):

1. **NessunDatoPerso**: leggendo il foglio di adesso più la coda del quaderno, ogni documento e ogni
   voce di indice è esattamente quella dell'ultima scrittura confermata (fantasma `tr`, la verità
   scritta a parte). Comprende "un cliente cancellato non riappare" nel database vivo: dopo la
   cancellazione la verità di X è "niente".
2. **FoglioIntegro**: ogni scatola nel foglio di adesso esiste ancora.
3. **IndicePuntaDoc** (lettori): se un lettore trova una voce di indice con valore v, il documento
   letto nella stessa istantanea c'è e ha valore v.
4. **DocHaIndice** (lettori): se il documento letto ha valore v, la voce di indice v c'era.
5. **UnaVersione** (lettori): quello che legge un lettore è esattamente la verità alla sua posizione S,
   per tutta la lettura.
6. **LettoreMaiSenzaScatola**: un lettore dentro il suo tetto di tempo non trova mai "scatola non
   trovata".
7. **RamoCorretto**: un ramo mostra esattamente la verità alla posizione in cui è nato. Comprende "un
   cliente cancellato non riappare da un ramo creato dopo".
8. **RamoIntegro**: le scatole della spilla di un ramo esistono sempre.

Vivezza (con l'equità: chi può lavorare prima o poi lavora; le scritture finiscono perché il
quaderno ha MaxSeq posizioni):

9. **NessunOrfanoPerSempre**: ogni scatola che non è nel foglio, non è in una spilla e non è di un
   lavoro in corso prima o poi viene cancellata.
10. **PileSottoIlTetto**: prima o poi, e per sempre, ogni cliente ha al massimo Cap pile nel foglio.
11. **CancellazioneFinita**: prima o poi la lista dei cancellati del foglio torna vuota, cioè la
    riscrittura mirata ha tolto ogni dato vecchio dei clienti cancellati dal foglio.

## I controlli negativi (tolgo una regola, TLC deve trovare l'errore)

- **NoInputCheck**: il lavaggio scambia il foglio anche se le sue scatole di partenza non ci sono
  più. Atteso: un cliente cancellato riappare da un lavaggio partito prima della cancellazione
  (viola NessunDatoPerso).
- **BlindPut**: il lavaggio riscrive il foglio intero da quello letto all'inizio (senza scrittura
  condizionale). Atteso: si perde una scatola dello scarico (viola NessunDatoPerso).
- **DropTombAlways**: il lavaggio toglie le lapidi anche se sotto c'è una versione più vecchia.
  Atteso: un documento cancellato o una voce di indice spenta tornano (viola NessunDatoPerso).
- **SplitSwap**: il lavaggio scambia il foglio in due scritture (prima i documenti, poi gli indici).
  Atteso: per un attimo un documento è senza voce di indice (viola NessunDatoPerso o DocHaIndice).
- **ReaderTwoSnap**: il lettore rilegge il foglio tra indice e documento. Atteso: viola
  IndicePuntaDoc o UnaVersione.
- **ForkNoLedger**: il ramo non eredita la lista dei cancellati. Atteso: viola RamoCorretto.
- **GcNoPins**: la pulizia non guarda le spille dei rami. Atteso: viola RamoIntegro.
- **Grace = 0**: la pulizia senza età minima. Atteso: viola LettoreMaiSenzaScatola.
- **GcOnlyRemoved**: la pulizia cancella solo scatole uscite da un foglio (non quelle mai entrate).
  Atteso: dopo un crash restano orfane per sempre (viola NessunOrfanoPerSempre).
- **MinSources = 3**: il lavaggio parte solo con almeno 3 scatole (come il lavaggio per taglia di
  SlateDB). Atteso: un cliente resta a 2 pile per sempre con tetto 1 (viola PileSottoIlTetto).

## I modelli ridotti

- **piccolo** (sicurezza): 2 clienti, 1 collezione con 1 indice, valori {1, 2}, 4 posizioni del
  quaderno, 5 scatole (i nomi si riusano dopo la cancellazione), 1 lavaggio, 1 lettore, 1 ramo,
  1 crash, tetto 1 pila, Grace 2, Hold 1, RHold 1, FHold 1.
- **largo** (sicurezza): come piccolo ma 5 posizioni, 6 scatole, 2 lavaggi in parallelo.
- **collezioni** (sicurezza): 2 clienti, 2 collezioni, valori {1}, 4 posizioni, 6 scatole.
- **vivo** (vivezza): come piccolo, con le proprietà 9–11 e l'equità.
- Un modello di "raggiungibilità" che controlla che i casi interessanti succedano davvero (lavaggio
  che fallisce lo scambio dopo una cancellazione, ramo nato dopo una cancellazione, crash con orfano
  poi pulito, lettore che finisce mentre un lavaggio scambia il foglio).

Cosa NON è nel modello (detto prima): la raccolta di famiglia tra più database e rami di rami (già
verificata in tla-rami), la replica del quaderno (tla-registro), il bancone (cache) che non cambia la
correttezza perché le scatole non cambiano mai, le scritture e il lavaggio dentro il ramo, e il taglio
delle scatole per dimensione (un cliente sta in una scatola per pila).
