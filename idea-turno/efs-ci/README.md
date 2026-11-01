# efs-ci: la micro-prova EFS da GitHub Actions (S13)

- `../../.github/workflows/efs-micro.yml`: parte con un push che cambia `comando.json` sul branch `idea-turno`.
  `{"modo":"permessi"}` = solo controllo dei permessi (0 $); `{"modo":"prova"}` = controllo e, se passa, la prova
  della scheda S12; `{"modo":"smonta"}` = solo pulizia e verifica.
- `permessi.sh`: letture, `--dry-run` di EC2, file system e bucket vuoti creati e cancellati subito.
- `prova.py`: montaggio con AWS CLI, fasi della scheda, riga SPESA ogni minuto, stop a 12 $, LIVE.md ogni ~10 min.
- `pulizia.sh`: toglie tutto `fenvix-prova-efs-*` e verifica; gira sempre.
- `LIVE.md` e `risultati/<run>/`: scritti dal workflow su questo branch.
- Permessi da aggiungere: `../../docs/efs-micro-permessi.md`.
