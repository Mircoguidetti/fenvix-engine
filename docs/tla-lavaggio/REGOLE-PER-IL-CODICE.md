# Regole che il codice del pezzo 1 (lavaggio per cliente) deve rispettare

Uscite da `LavaggioPerCliente.tla` (sessione M28, 2026-11-03). Una regola per riga. Tra parentesi
quadre il controllo negativo che mostra cosa succede senza la regola, quando c'è; "(da tla-rami)" se
la regola era già stata verificata là e qui è solo riusata.

## Scrittura e quaderno (log)

1. Un documento e tutte le sue voci di indice sono un solo record del quaderno, con una sola posizione.
2. La cancellazione di un cliente è anch'essa un record del quaderno, "cliente X fino alla posizione E"; si conferma al cliente quando il record è confermato.
3. Ogni voce dentro una scatola porta la posizione del quaderno che l'ha scritta, e chi legge tiene sempre la versione con la posizione più alta tra tutte le scatole lette (non "la prima pila che la contiene"): è questo che lascia il lavaggio libero di unire pile qualsiasi, anche non vicine in età.
4. Il quaderno butta una posizione già scaricata solo dopo un ritardo più lungo del tetto di tempo di un lettore.

## Scarico (flush)

5. Lo scarico scrive la scatola, poi in un solo PUT del foglio aggiunge la scatola, sposta la posizione scaricata e porta le cancellazioni del suo tratto nella lista dei cancellati del foglio.
6. Lo scarico non scrive in scatola i dati coperti da una cancellazione dello stesso tratto.

## Foglio indice (manifest)

7. Il foglio si scrive solo con PUT condizionale (If-Match); se fallisce si rilegge il foglio nuovo e si riapplica la propria modifica solo se le sue condizioni valgono ancora, mai si scrive un foglio costruito da una copia vecchia. [BlindPut: si perde una scatola dello scarico]
8. Documenti e voci di indice dello stesso giro di lavaggio entrano nel foglio con lo stesso PUT. [SplitSwap: un documento resta senza voce di indice]

## Lavaggio (compaction) per cliente

9. Il lavaggio parte per cliente appena quel cliente supera il tetto di pile, anche se le sue scatole sono solo 2 (non aspetta un numero minimo di scatole come il lavaggio per taglia di SlateDB). [MinSources = 3: un cliente resta sopra il tetto per sempre]
10. Il lavaggio scrive una scatola per cliente (taglia ai confini tra clienti) e non cancella mai niente.
11. Lo scambio del lavaggio riesce solo se tutte le sue scatole di partenza sono ancora nel foglio; altrimenti rinuncia, e le scatole che ha scritto le toglierà la pulizia. [NoInputCheck: un cliente cancellato riappare da un lavaggio partito prima]
12. Il lavaggio tiene per ogni chiave solo la versione più nuova e toglie una lapide (tombstone) solo se, nel foglio letto all'inizio, fuori dalle sue scatole non c'è una versione più vecchia della stessa chiave. [DropTombAlways: un documento cancellato torna]
13. Il lavaggio scarta i dati coperti dalla lista dei cancellati che ha letto all'inizio.
14. Il lavaggio ha un tetto di tempo (Hold) minore dell'età minima della pulizia: oltre il tetto rinuncia e ricomincia da capo (da tla-rami).

## Cancellazione mirata di un cliente (GDPR)

15. La riscrittura mirata prende solo le scatole del foglio con dati del cliente più vecchi di E, le riscrive senza quei dati e le scambia con le stesse regole 7 e 11.
16. La riga "cliente X fino a E" si toglie dalla lista dei cancellati solo con un PUT condizionale del foglio che verifica che nessuna scatola del foglio contenga più dati di X con posizione minore di E.
17. Lettori, rami e lavaggi filtrano sempre con la lista dei cancellati più le cancellazioni della coda del quaderno fino alla loro posizione.

## Lettori

18. Un lettore prende una sola istantanea (foglio + posizione S del quaderno) e legge indice e documento solo da quella, per tutta la lettura. [ReaderTwoSnap: voce di indice senza il suo documento]
19. Un lettore ha un tetto di tempo (RHold) non più lungo dell'età minima della pulizia; oltre, ricomincia con un'istantanea nuova (da tla-rami; meglio ancora con "su 404 rileggi il foglio").

## Rami (fork)

20. Il ramo copia il foglio, la coda del quaderno fino alla sua posizione S e la lista dei cancellati. [ForkNoLedger: un cliente cancellato riappare in un ramo creato dopo]
21. Il ramo mette la spilla entro FHold, minore dell'età minima; se sfora ricomincia (DECISIONS 2026-11-02, da tla-rami).

## Pulizia (GC)

22. La pulizia cancella una scatola solo se non è nel foglio, non è nella spilla di un ramo e ha l'età minima. [GcNoPins: il ramo perde una scatola]
23. L'età minima si conta da quando la scatola è nata o è uscita dall'ultimo foglio o spilla, e la scrive chi la toglie (da tla-rami). [Grace = 0: un lettore trova "scatola non trovata"]
24. La pulizia cancella anche le scatole mai entrate in un foglio (orfane di un crash o di uno scambio fallito), con la stessa età minima. [GcOnlyRemoved: dopo un crash restano orfane per sempre]

## Cosa resta fuori (non verificato qui)

- Rami e punti di ripristino nati prima della cancellazione tengono ancora, con la loro spilla, le scatole con i dati del cliente cancellato. Per il GDPR la cancellazione va propagata a tutta la famiglia (una riga nella lista dei cancellati di ogni ramo vivo e la sua riscrittura mirata) e i punti di ripristino più vecchi di E vanno tolti o filtrati. Non modellato.
- Il quaderno replicato, il bancone, la raccolta tra più database (tla-registro, PR #47, tla-rami).

## Quante sessioni (STIMATO)

Le regole 1, 2, 4, 5, 7 ci sono già in parte nel motore (batch unico, PUT condizionale di SlateDB). Il resto è codice nuovo intorno al lavaggio e alla pulizia: oggi lavaggio e pulizia sono quelli di SlateDB 0.17, che contano l'età dalla nascita (tla-rami) e non sanno niente di clienti né di lista dei cancellati.

- Lista dei cancellati nel foglio, record di cancellazione nel quaderno, filtro in lettura, scarico, lavaggio e ramo (regole 2, 5, 6, 13, 17, 20): 2–3 sessioni.
- Lavaggio per cliente con scambio condizionale, controllo delle scatole di partenza, regola delle lapidi, tetto di tempo, scatole tagliate per cliente, partenza per cliente sopra il tetto (regole 3, 8–12, 14): 3–5 sessioni.
- Riscrittura mirata e chiusura della cancellazione (regole 15, 16): 1–2 sessioni.
- Pulizia con età dall'uscita, orfani dei crash, spille dei rami, ritardo del troncamento del quaderno (regole 4, 22–24): 1–2 sessioni.
- Prove: confronto con mongod durante il lavaggio, SIGKILL a metà, test che rifanno le tracce dei 10 controlli negativi: 2–3 sessioni.

Totale: circa 9–15 sessioni Claude Code (STIMATO), dentro le 15–25 del pezzo 1 di `PLAN-LAVAGGIO.md`.
