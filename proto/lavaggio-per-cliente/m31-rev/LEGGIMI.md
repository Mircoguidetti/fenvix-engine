# Revisore M31: prove locali (branch rev-m31, solo lettura di freddo-s3)

Programma: `proto/lavaggio-per-cliente/m31` di freddo-s3 (commit e843564), costruito senza modifiche.

1. Carico piccolo: 0,6 GB, 300 clienti, 30% dormienti, stessi parametri di esegui.sh tranne taglia e ritmo (`risultati/carico-0.6gb.jsonl`).
2. `copia` su moto (S3 finto, porta 5055) e `tempi --magazzino s3 --finto 25:0.6:90`, varianti `parallelo` e
   `parallelo+bancone+doppia+ra256`, 30 clienti per gruppo (`risultati/tempi-moto-finto.*`).
   Conteggio: il contatore del programma dice 2.445 richieste di classe GET (609 riscaldo + 1.836 misure, 32 doppie
   comprese); moto ne ha viste 2.421 (2.405 GET + 16 HEAD, `richieste-viste-da-moto.txt`).
3. Secondo giro dei clienti attivi: `tetto-4.patch` porta da 4 a 64 il numero di scatole che SlateDB apre insieme per
   ogni gruppo (L0 e sorted run). Ritardo fisso 20 ms, disco locale: con 4 i clienti attivi (8 GET) fanno Q1 44,4 ms,
   con 64 fanno 22,9 ms (`tetto4-contro-64.jsonl`, etichette base4 e cap64).

Comandi esatti nel rapporto del revisore: fenvix-brain `sessions/2026-11-04-revisore-m31/RAPPORTO.md`.
