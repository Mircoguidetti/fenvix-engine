# P1 · giri dall'Irlanda su Atlas M10 e M30, versione 3 (2026-11-05)

Da una macchina EC2 in eu-west-1, accanto ad Atlas, con lo script del fondatore `prova_infinite.py` p1-1.6.2 NON
modificato (md5 8396ca455557baaa4e3a9b1f881451f9, controllato dal workflow, da simula_programma.py e dalla macchina).
Passi per il fondatore e rapporto: fenvix-brain, `sessions/2026-11-05-analisi-irlanda/` (PASSI.md, RAPPORTO.md).
Versioni prima: v1 `sessions/2026-11-05-p1-code-irlanda/`, v2 `sessions/2026-11-05-p1-irlanda-2/` (comprese nella v3).

- `programmi/NOME/passi.txt` e `macchina.txt`: i sette programmi (carica, m10-a, m10-b, m10-massimo, m30-a, m30-b,
  m30-massimo); comando.json = `{"modo": "giro", "programma": "NOME"}`.
- `stati/`: i file di stato, uno per database, e `CATENA.txt` (md5). Li aggiorna SOLO il workflow, alla fine di ogni
  giro; prima di spendere scrive "IN CORSO" (se un giro non li riporta, il giro dopo si ferma).
- `programma.py` (macchina): esegue i passi, sposta il giro di prima nel file di stato, passo `dormienti`, copie del
  carico massimo, CPU di ogni processo e `validita-macchina.txt`.
- `simula_programma.py` (runner e prove): tutto il programma a secco con lo script vero e il consumo simulato.
- `.github/workflows/code-irlanda.yml`, `esegui.sh`, `ci/`, `s3io.py`, `attendi_db.py`, `cpu_macchina.py`,
  `controlla_parallelo.py`, `somma_massimo.py`: il giro (modello M31 e v2).
- `prova-locale/prova-v3.sh` (con `rimpicciolisci.py`, `conta_letti.py`): la prova a 0 $ con AWS finto (moto) e il
  nostro mongo-server al posto di Atlas, sui sette programmi rimpiccioliti.
- Dalla v2, non piu' usati: `opzioni.txt`, `massimo.txt`, `copie.txt`, `prepara_stato.py`, `controlla_stato.py`.
