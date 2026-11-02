# Prova della manopola delle zone (N1, branch prova-zone): soglie scritte PRIMA delle prove

Scritte il 2026-11-02, prima di qualsiasi misura. Non si cambiano dopo: se una soglia si rivela sbagliata lo dice il
rapporto, e la soglia resta quella scritta qui.

## Cosa si prova

1. Copia grezza del quaderno su S3 (MinIO, o il suo sostituto locale moto se il binario non c'è) ogni secondo,
   fatta dal capo: un oggetto per tratto, con inizio e fine (indice e posizione) nell'intestazione e un CRC.
2. Ripristino di una cella da zero: cartelle nuove e vuote, si leggono i fogli indice (manifest dello scaricatore) e
   i tratti; il ripristino si ferma al primo buco.
3. Prove: (a) morte della zona = SIGKILL di tutte e 3 le repliche e dello scaricatore, poi cancellazione delle loro
   cartelle; (b) un oggetto mancante nel mezzo della catena; (c) S3 lento e S3 fermo per 2 minuti, con il ritardo
   della copia misurato.

## Finestra dichiarata

- **Normale:** una scrittura confermata è su S3 entro **1,5 s** dalla conferma (1 s di intervallo della copia più
  il PUT). Quindi alla morte della zona all'istante K si possono perdere solo scritture confermate dopo K − 1,5 s.
- **S3 lento o fermo:** la finestra è quella che la metrica `copia_ritardo_ms` del capo mostrava all'ultima lettura
  prima di K, più 1,5 s. La metrica deve dire la verità: la perdita vera non può essere più vecchia di così.

## Soglie (dal prompt N1, punto 4)

- **S1, zero scritture confermate perse oltre la finestra dichiarata.** Si controlla con le scritture testimoni
  (chiave unica, una per scrittura): ogni testimone confermato prima di K − finestra deve essere nella cella
  ripristinata (nel quaderno ripristinato o negli SST dei manifest) con la sua posizione e il suo valore. Zero
  eccezioni. Anche zero testimoni con posizione diversa da quella confermata (scritture doppie o spostate).
- **S2, il ripristino si ferma al buco.** Con un oggetto tolto nel mezzo, il quaderno ripristinato finisce
  esattamente all'indice prima del buco: nessuna voce dopo il buco viene applicata, e gli oggetti dopo il buco
  vengono messi da parte (non riusati dalla cella nuova).
- **S3, il ritardo della copia è visibile in una metrica.** Con S3 fermo 2 minuti, `copia_ritardo_ms` nello `status`
  del capo cresce di circa 1 s al secondo (entro il 20%) per tutta la durata del fermo, fa scattare l'allarme oltre
  5 s, e torna sotto 2 s entro 60 s dalla ripresa di S3.

## Misure riportate senza soglia di vittoria (dalla prova 1 di Fable)

PUT al secondo verso S3 a regime; CPU del thread della copia; p99 dei commit con e senza copia; tempo di ripristino
con 10.000 database (misura il nostro codice in locale, NON l'RTO vero, che dipende dal lancio delle macchine e dal
riscaldamento del bancone).

Solo prova, nessuna decisione di prodotto.
