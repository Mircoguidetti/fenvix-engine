# K2 · la cucina minima su AWS (2026-11-05)

Il nostro server (`mongo-server`, dati solo su S3) su una EC2 in eu-west-1, contro lo script del fondatore
`prova_infinite.py` p1-1.6.2 NON modificato (md5 8396ca455557baaa4e3a9b1f881451f9, controllato sulla macchina), da una
seconda EC2 nella stessa zona, con lo stesso metodo del giro dall'Irlanda su Atlas M30 (branch `code-irlanda`).
Passi del fondatore e rapporto: fenvix-brain, `sessions/2026-11-05-k2-cucina-minima/` (PASSI.md, RAPPORTO.md).

- `.github/workflows/cucina-minima.yml`: il workflow (innesco: commit di `cucina/comando.json`; modi `permessi`,
  `giro`, `smonta`; campi facoltativi `taglie`, `motore`, `gb`, `cliente`, `copie`, spiegati in testa al file).
- Esempio: `{"modo": "giro", "taglie": ["c7i.large", "m7i.large"], "motore": "server-per-script"}`.
- `carica.txt`: il caricamento (come su Atlas: `carica --gb 10`), solo alla prima taglia.
- `opzioni.txt`: il giro di ogni taglia (le opzioni dell'Irlanda; `--giro cm_<K>_<taglia>` lo aggiunge la macchina).
- `massimo.txt`: ogni copia del carico massimo (piu' `--file`, `--cartella-dati`, `--giro` di ogni copia).
- `server.txt`: opzioni in piu' del nostro server (vuoto: il motore com'e').
- `ci/`: lato runner. `giro-ci.sh` (la giornata), `permessi.sh` (0 $, anche la quota di vCPU e la scelta della
  macchina cliente), `tipi.py` (taglie ammesse, vCPU, prezzi DICHIARATI), `certificati.sh` (CA e certificati TLS),
  `user-data.sh`, `pulizia.sh`, `recupera.sh`, `controllo-finale.sh`, `controlla_segreti.py`.
- Lato macchine: `esegui-cliente.sh` (caricamento, giro, carico massimo, per ogni taglia), `esegui-server.sh` (il
  nostro server con TLS e utenti), `crea_utente.py`, `s3io.py`, `attendi_db.py`, `prepara_stato.py`, `cpu_macchina.py`,
  `controlla_parallelo.py`, `somma_massimo.py` (gli ultimi tre copiati dall'Irlanda).
- `prova-locale/`: la prova a 0 $ con AWS finto (moto) e il nostro mongo-server vero su S3 finto.
