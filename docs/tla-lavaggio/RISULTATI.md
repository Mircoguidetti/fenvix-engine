# Lavaggio per cliente: risultati di TLC

Sessione M28, 2026-11-03. TLC vero (tla2tools.jar ufficiale, release v1.8.0 su GitHub, build
2026.11.02), Java 21, macchina da 4 core e 15 GB. Ogni numero di stati è MISURATO e viene dagli
output salvati in `output/`. Per rifare: `python3 gen.py`, poi `./run.sh <modello>`.

## Modelli giusti (tutte le regole accese): nessun errore

Invarianti controllate in ogni modello: NessunDatoPerso, FoglioIntegro, IndicePuntaDoc, DocHaIndice,
UnaVersione, LettoreMaiSenzaScatola, RamoCorretto, RamoIntegro. Simmetria tra i due clienti (A e B
sono intercambiabili, TLC conta una volta sola due stati uguali a meno dello scambio).

Base di tutti: 2 clienti, tetto 1 pila, età minima 2 giri, tetti di tempo 1 giro, 1 crash, 5
scatole (6 dove serve), nomi delle scatole riusati dopo la cancellazione.

- `lettori`: 1 collezione, un valore che cambia (1, poi 2, con la voce di indice vecchia spenta e la
  nuova accesa), 2 posizioni del quaderno, 1 lavaggio, 1 lettore. 502.123 stati distinti, profondità
  42, ricerca completa, nessun errore.
- `lettori3`: come sopra ma 3 posizioni con un valore solo (crea, cancella, ricrea). 4.446.591
  stati, profondità 48, completa, nessun errore.
- `cancella`: lavaggio partito prima della cancellazione: due valori, 3 posizioni, 1 cancellazione
  di un cliente, riscrittura mirata, nessun lettore né ramo. 2.009.427 stati, profondità 47,
  completa, nessun errore.
- `cancella-ramo`: un valore, 3 posizioni, 1 cancellazione, 1 ramo (prima o dopo la cancellazione).
  4.316.286 stati, profondità 49, completa, nessun errore.
- `due-lavaggi`: due lavaggi in parallelo, un valore, 3 posizioni, 1 cancellazione, 6 scatole.
  6.234.617 stati, profondità 60, completa, nessun errore.
- `collezioni`: 2 collezioni con il loro indice, un valore, 3 posizioni, 1 cancellazione, 6
  scatole. 4.207.441 stati, profondità 55, completa, nessun errore.
- `tutto`: tutti i pezzi insieme (scrittore, scarico, lavaggio, lettore, ramo, cancellazione,
  riscrittura mirata, pulizia, crash), un valore, 2 posizioni. 2.178.306 stati, profondità 34,
  completa, nessun errore.

Totale dei modelli di sicurezza completi: 23.894.791 stati distinti, nessun errore.

## Vivezza: nessun errore

Proprietà: NessunOrfanoPerSempre, PileSottoIlTetto, CancellazioneFinita, con l'equità scritta in
`Fair` (chi può lavorare prima o poi lavora; il lavaggio, lo scarico e la riscrittura mirata
riescono se hanno l'occasione infinite volte). Senza simmetria (TLC non la ammette con la vivezza).

Base: 2 clienti, un valore, tetto 1 pila, 1 crash, nessun lettore (i lettori non servono alla
vivezza: la pulizia non li aspetta).

- `vivo`: 3 posizioni, 1 cancellazione, 4 scatole, nessun ramo. 237.063 stati distinti,
  profondità 41, completa, nessun errore.
- `vivo-ramo`: 2 posizioni, 1 ramo che può restare vivo per sempre, 5 scatole. 91.313 stati,
  profondità 30, completa, nessun errore.

## Controlli negativi: tutti trovano l'errore

Ognuno toglie una regola e gira sul modello più piccolo dove quella regola conta.

Tra parentesi: stati distinti visti da TLC fino all'errore (con 4 worker; con un numero diverso di
worker il conteggio cambia, conta solo "trovato sì o no") e lunghezza della traccia.

- **NoInputCheck** (lo scambio del lavaggio non guarda se le sue scatole ci sono ancora): violata
  NessunDatoPerso (261.980 stati, traccia di 17 stati). In parole: il cliente A scrive 2, poi 1; due
  scarichi fanno due scatole; il lavaggio legge le due scatole; arriva la cancellazione di A; lo
  scarico la porta nella lista dei cancellati; la riscrittura mirata toglie le due scatole e la riga
  si chiude; il lavaggio, partito prima, scrive la sua scatola con "A = 1" e la mette nel foglio. A
  riappare. Regola che lo corregge: 11 (lo scambio riesce solo se tutte le scatole di partenza sono
  nel foglio).
- **BlindPut** (il lavaggio scrive il foglio intero letto all'inizio): violata NessunDatoPerso
  (7.278 stati, traccia di 14). In parole: il lavaggio legge il foglio; uno scarico aggiunge una
  scatola e sposta la posizione scaricata; il quaderno tronca; il lavaggio riscrive il foglio
  vecchio: la scatola dello scarico sparisce e la sua scrittura non è più nemmeno nel quaderno.
  Regola: 7.
- **DropTombAlways** (lapidi tolte sempre): violata NessunDatoPerso (25.429 stati, traccia di 12).
  A scrive 1, poi 2, poi cancella il documento; tre scatole; il lavaggio unisce le due più nuove e
  butta la lapide; sotto resta "A = 1", che torna visibile. Regola: 12.
- **SplitSwap** (prima i documenti, poi gli indici, in due scritture del foglio): violata
  NessunDatoPerso (14.493 stati, traccia di 11). Dopo la prima scrittura il foglio ha il documento
  di A ma non la sua voce di indice. Regola: 8.
- **ReaderTwoSnap** (il lettore rilegge il foglio prima del documento): violata DocHaIndice (421
  stati, traccia di 5). Il lettore legge l'indice vuoto; A scrive 1; il lettore legge il documento
  dalla vista nuova: c'è un documento senza voce di indice. Regola: 18.
- **ForkNoLedger** (il ramo non eredita la lista dei cancellati): violata RamoCorretto (11.976
  stati, traccia di 9). A scrive 1; scarico; A viene cancellato; lo scarico porta la cancellazione
  nella lista; il ramo nasce dopo e mostra ancora A. Regola: 20.
- **GcNoPins** (la pulizia non guarda la spilla del ramo): violata RamoIntegro (90.416 stati,
  traccia di 14). Il ramo fissa una scatola; il lavaggio la toglie dal foglio; dopo due giri la
  pulizia la cancella anche se il ramo la usa. Regola: 22.
- **Grace = 0** (nessuna età minima, lettore con tetto di 1 giro): violata LettoreMaiSenzaScatola
  (32.466 stati, traccia di 12). Il lettore prende il foglio; il lavaggio scambia; la pulizia
  cancella subito la scatola vecchia; il lettore la cerca e non la trova. Regola: 23.
- **GcOnlyRemoved** (la pulizia cancella solo scatole uscite da un foglio): violata
  NessunOrfanoPerSempre (10.190 stati, traccia di 9). Lo scarico scrive la scatola e va in crash
  prima di aggiornare il foglio; la scatola non entra mai in un foglio e resta per sempre. Regola: 24.
- **MinSources = 3** (il lavaggio parte solo con almeno 3 scatole): violata PileSottoIlTetto
  (12.840 stati, traccia di 10). A ha 2 pile con tetto 1 e le scritture finiscono: il lavaggio non
  parte mai. Regola: 9.

10 controlli negativi su 10 trovano l'errore previsto (8 di sicurezza, 2 di vivezza).

## Raggiungibilità: i casi interessanti succedono davvero

Ognuna di queste invarianti dice "non succede mai X" e TLC la deve trovare violata: così si vede che
il modello non è vuoto. Tutte e 9 violate come previsto (MISURATO): lavaggio che fallisce lo scambio
perché una cancellazione gli ha tolto le scatole; ramo nato dopo una cancellazione; crash con
scatola orfana; orfano poi tolto dalla pulizia; lettore che finisce mentre il foglio è cambiato;
scambio del lavaggio con un lettore in corso; cancellazione finita (riga tolta dalla lista); lapide
tolta dal lavaggio; cliente ricreato dopo la cancellazione.

## Corse interrotte (non complete, tenute per onestà)

Queste corse non sono finite (tagliate da me per tempo): nessun errore fino al punto raggiunto,
ma non contano come verifica completa.

- `lettori` a 3 posizioni con due valori: 34.279.087 stati, profondità 25, nessun errore.
- `cancella-ramo` con due valori: 22.229.976 stati, profondità 24, nessun errore.
- `due-lavaggi` con due valori: 23.221.122 stati, profondità 24, nessun errore.
- `vivo-ramo` a 3 posizioni (vivezza con il ramo): 3.589.053 stati, nessun errore fino al taglio.
- `lettori` a 4 posizioni, su una versione più vecchia della specifica (senza troncamento del
  quaderno e senza simmetria): 18.275.612 stati, profondità 15, nessun errore.

## Cosa ho cambiato nel modello mentre lavoravo, e perché

1. Il primo modello non troncava mai il quaderno: un foglio riscritto "all'indietro" (BlindPut) si
   rileggeva dal quaderno e TLC non trovava l'errore. Ho aggiunto il troncamento del quaderno con un
   giro di ritardo. È una regola vera per il codice (regola 4): il quaderno butta una posizione solo
   dopo il tetto di tempo di un lettore.
2. Il ramo ora copia la coda del quaderno e le cancellazioni della coda al momento del fork (prima
   le leggeva dal quaderno, che ora si tronca).
3. Il lettore ora fallisce se una qualsiasi scatola del suo foglio è stata cancellata, non solo quelle
   che gli servono: è più severo, e così la pulizia può dimenticare il contenuto delle scatole
   cancellate (meno stati).
4. Il negativo "Grace = 0" all'inizio non mordeva perché avevo messo anche il lettore a 0 giri: il
   lettore scadeva nello stesso giro della cancellazione. Con il lettore a 1 giro TLC trova l'errore.
5. Il negativo NoInputCheck all'inizio non mordeva sul modello con un solo valore: il caso ha bisogno
   di due versioni dello stesso documento. Con due valori TLC lo trova.
6. La proprietà "nessun orfano per sempre" scritta come "per ogni scatola, orfana porta a
   cancellata" rendeva la verifica di vivezza molto lenta; ora è "prima o poi nessuna scatola è
   orfana, per sempre", che con scritture e crash finiti è la stessa cosa e un po' più forte.

## Limiti

- I modelli sono piccoli: 2 clienti, 2–3 posizioni del quaderno, 1–2 valori, 1 lettore, 1 ramo,
  1 crash. Gli errori dei negativi stanno tutti entro 16 passi; un errore che ha bisogno di molte
  più scritture non si vedrebbe.
- Le letture e le scritture di una scatola sono atomiche nel modello; nella realtà durano, e sono
  protette dalle stesse età (tetto di tempo minore dell'età minima).
- Non modellati: rami e punti di ripristino nati prima della cancellazione (vedi
  REGOLE-PER-IL-CODICE.md, "Cosa resta fuori"), lavaggio e scritture dentro il ramo, quaderno
  replicato, bancone.
