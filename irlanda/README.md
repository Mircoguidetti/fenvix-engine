# P1 · giro corto dall'Irlanda (2026-11-05)

Q2, Q3 e Q1 su Atlas M30 da una macchina EC2 c7i.xlarge in eu-west-1, accanto ad Atlas, con lo script del fondatore
`prova_infinite.py` p1-1.6.2 NON modificato (md5 8396ca455557baaa4e3a9b1f881451f9, controllato dal workflow e dalla
macchina). Il comando dello script è in `opzioni.txt`. Passi per il fondatore e rapporto: fenvix-brain,
`sessions/2026-11-05-p1-code-irlanda/` (PASSI.md, RAPPORTO.md).

- `.github/workflows/code-irlanda.yml`: il workflow (innesco: commit di `irlanda/comando.json`).
- `controlla_stato.py`: sul runner, prima di spendere: md5, file di stato, piano a secco, guardia contro un file vecchio.
- `ci/`: lato runner (giro-ci.sh, user-data.sh, permessi.sh; pulizia.sh, recupera.sh, controllo-finale.sh copiati da M31;
  controlla_segreti.py).
- `esegui.sh`, `s3io.py`, `attendi_db.py`: lato macchina.
- `prova-locale/`: prova a 0 $ con AWS finto (moto) e il nostro mongo-server al posto di Atlas.
