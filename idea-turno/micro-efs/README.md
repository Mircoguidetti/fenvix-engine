# micro-efs

Il programma della micro-prova del quaderno a turno su Amazon EFS (S12). Rust, una sola dipendenza (`libc`).
La regola che esercita è in `../../docs/turno.md`; i passi su AWS sono nella scheda
`fenvix-brain/sessions/2026-11-01-s12-micro-efs/SCHEDA.md`.

```bash
cargo build --release
./target/release/micro-efs aiuto
```

Comandi:
- `scrivi`, `matrice`: latenza delle scritture in coda (fsync o O_DSYNC, lotti da 1 a 256 KiB, da 1 a 64 file),
  con controllo del turno spento, dopo ogni lotto (`si`) o in pipeline; domanda con `open`, `stat` o `odirect`;
  `--solo-controllo si` per misurare le sole domande. Ogni fase scrive una riga in `--fasi` con inizio e fine.
- `leggi-coda`: da un'altra istanza, pread da 4 KiB (O_DIRECT o no) sul libro che un'altra scrive, e dopo quanti
  ms si vedono i lotti nuovi.
- `gara`, `gara-verifica`: banco 9, esattamente un vincitore per round.
- `presa-titolare`, `presa-subentra`, `presa-verifica`: banco 10 (`--modo uccidi`) e zombie (`ferma`, `vivo`).
- `metriche`: per ogni fase legge da CloudWatch (AWS CLI) TotalIOBytes, MeteredIOBytes, DataWriteIOBytes,
  DataReadIOBytes, MetadataIOBytes, MetadataReadIOBytes, MetadataWriteIOBytes (Sum e SampleCount), e scrive KiB per
  operazione, costo per lotto e conto al mese.

Prova in locale: `./prova-locale.sh [cartella] [cartella-di-controllo]`. In questo ambiente non c'è il client NFS
del kernel (`/proc/filesystems` senza nfs, niente moduli): la prova locale gira su disco locale ext4. Controlla
la logica (record, verifiche, segnali tra processi, SIGKILL/SIGSTOP), non la semantica NFS: quella si controlla
nei primi minuti su AWS (scheda, passo 0).
