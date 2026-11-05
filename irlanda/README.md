# P1 · giro dall'Irlanda su Atlas M30, versione 2 (2026-11-05)

Da una macchina EC2 c7i.8xlarge in eu-west-1, accanto ad Atlas, con lo script del fondatore `prova_infinite.py`
p1-1.6.2 NON modificato (md5 8396ca455557baaa4e3a9b1f881451f9, controllato dal workflow e dalla macchina):
Q2, Q3, Q1 (50 clienti mai letti ciascuna), parallelo 10 e 50 insieme (180 clienti), poi carico_massimo con 20 copie
dello script insieme. Passi per il fondatore e rapporto: fenvix-brain, `sessions/2026-11-05-p1-irlanda-2/`
(PASSI.md, RAPPORTO.md, PASSO-7c.md). Versione 1 (solo Q2, Q3, Q1): `sessions/2026-11-05-p1-code-irlanda/`.

- `.github/workflows/code-irlanda.yml`: il workflow (innesco: commit di `irlanda/comando.json`).
- `opzioni.txt`: il comando del giro sul file di stato vero. `massimo.txt`: il comando di ogni copia del carico
  massimo (in piu' `--file`, `--cartella-dati`, `--giro` di ogni copia, scritti da esegui.sh).
- `prepara_stato.py`: sul runner, sposta il giro M30 sotto `giro_finale_prima_di_irlanda` (se no il parallelo salta i
  livelli 10 e 50).
- `controlla_stato.py`: sul runner, prima di spendere: md5, file di stato, piani a secco, guardia contro un file vecchio.
- `ci/`: lato runner (giro-ci.sh, user-data.sh, permessi.sh con la quota di vCPU; pulizia.sh, recupera.sh,
  controllo-finale.sh copiati da M31; controlla_segreti.py).
- `esegui.sh`, `s3io.py`, `attendi_db.py`, `cpu_macchina.py`, `controlla_parallelo.py`, `somma_massimo.py`: lato
  macchina.
- `prova-locale/`: prove a 0 $ con AWS finto (moto) e il nostro mongo-server al posto di Atlas
  (`prova-giro-locale.sh`; `prova-parallelo.sh` con `proxy_ritardo.py` per la rete del Mac e dell'Irlanda).
