# Lavaggio per cliente: risultati di TLC

Sessione M28, 2026-11-03. TLC vero (tla2tools.jar ufficiale, release v1.8.0 su GitHub, build
2026.11.02), Java 21, macchina da 4 core e 15 GB. Ogni numero di stati è MISURATO e viene dagli
output salvati in `output/`. Per rifare: `python3 gen.py`, poi `./run.sh <modello>`.

## Modelli giusti (tutte le regole accese): nessun errore

Invarianti controllate in ogni modello: NessunDatoPerso, FoglioIntegro, IndicePuntaDoc, DocHaIndice,
UnaVersione, LettoreMaiSenzaScatola, RamoCorretto, RamoIntegro. Simmetria tra i due clienti (A e B
sono intercambiabili, TLC conta una volta sola due stati uguali a meno dello scambio).

@@GIUSTI@@

## Vivezza: nessun errore

Proprietà: NessunOrfanoPerSempre, PileSottoIlTetto, CancellazioneFinita, con l'equità scritta in
`Fair` (chi può lavorare prima o poi lavora; il lavaggio, lo scarico e la riscrittura mirata
riescono se hanno l'occasione infinite volte). Senza simmetria (TLC non la ammette con la vivezza).

@@VIVI@@

## Controlli negativi: tutti trovano l'errore

Ognuno toglie una regola e gira sul modello più piccolo dove quella regola conta.

@@NEGATIVI@@

## Raggiungibilità: i casi interessanti succedono davvero

Ognuna di queste invarianti dice "non succede mai X" e TLC la deve trovare violata: così si vede che
il modello non è vuoto. Tutte e 9 violate come previsto (MISURATO): lavaggio che fallisce lo scambio
perché una cancellazione gli ha tolto le scatole; ramo nato dopo una cancellazione; crash con
scatola orfana; orfano poi tolto dalla pulizia; lettore che finisce mentre il foglio è cambiato;
scambio del lavaggio con un lettore in corso; cancellazione finita (riga tolta dalla lista); lapide
tolta dal lavaggio; cliente ricreato dopo la cancellazione.

## Corse interrotte (non complete, tenute per onestà)

@@INTERROTTE@@

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
