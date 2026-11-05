# Q8 su AWS: le corsie del quaderno condiviso con dischi e zone veri

Pronta da lanciare, **non lanciata**. La autorizza il fondatore (spesa su AWS).

## Cosa fa

La stessa prova fatta in locale da `scripts/corsie.py` (vedi il rapporto Q8 in fenvix-brain), su macchine vere in
eu-west-1:

- tre c7i.xlarge, una per zona (a, b, c), ognuna con una replica del quaderno sul suo disco gp3 da 30 GB (fdatasync
  vero su EBS); la replica usa 2 vCPU (`taskset`), come una m7g.large della cella di Fable;
- una c7i.xlarge nella zona a con i generatori (i database normali a 100 scritture al secondo, i vicini rumorosi) e lo
  scaricatore finto (pota la finestra senza scrivere su S3);
- livelli 1 (due volte), 2, 20, 50, 100 database; le sette fasi (nessuno, 1.000/s, 20.000/s, raffiche da 50.000, record
  da 1 MB, vicino che muore e riparte, il 10% rumorosi insieme); 60 s di misura per fase (120 s con 1–2 normali);
- stessa configurazione del quaderno della prova locale R1: corsie, riserva 200 record/s per database, tetto di cella
  2 MB/s sopra la riserva, riciclo dei segmenti, finestra 10 s; in più la replica compressa (zstd 1) per pagare meno
  il traffico tra zone.

Cosa dice in più del locale: rete vera tra le zone (0,5–2 ms in più per la conferma, DICHIARATO AWS), dischi EBS
veri (uno per replica, con i loro limiti di IOPS e di banda), il rumore degli altri clienti AWS sulle stesse macchine
e sugli stessi dischi.

## Quanto costa (STIMATO)

- Macchine: 4 c7i.xlarge a 0,1915 $/h (DICHIARATO, doit.com, ripreso da M31) per circa 75 minuti: circa 1,0 $.
- Traffico tra zone: circa 13,5 milioni di record da 2,2 KB, al più 4 attraversamenti ciascuno, 0,02 $ per GB
  attraversato: al più 2,4 $ (con la compressione della replica probabilmente circa 1,2 $).
- Dischi, IPv4, S3: meno di 0,10 $.
- Totale: circa 2–3,5 $. Tetto: 5 $ (si ferma da solo e pulisce).

## Prima di lanciare (fondatore, console IAM, una volta)

1. Ruolo GitHub `fenvix-gha-tests`, trust policy: nella condizione su `token.actions.githubusercontent.com:sub`
   aggiungere anche `repo:Mircoguidetti/fenvix-engine:ref:refs/heads/quaderno-corsie` (oggi c'è solo `freddo-s3`).
2. La policy inline `fenvix-coldread-test` deve permettere `ec2:RunInstances` di quattro c7i.xlarge (NON SO se oggi
   ha un limite sul numero o sul tipo: il modo `permessi` lo controlla con `--dry-run` a 0 $).

I nomi in AWS sono quelli già autorizzati per M31: tag `project=fenvix-coldread`, ruolo della macchina
`fenvix-coldread-machine`, bucket `fenvix-coldread-q8-<run>` (la policy della macchina copre `fenvix-coldread-*`).

## Come si lancia

1. `{"modo": "permessi"}` in `proto/quaderno/aws-q8/comando.json`, commit e push sul branch `quaderno-corsie`:
   controllo a 0 $. Deve finire con "PERMESSI: tutti presenti".
2. `{"modo": "prova"}` nello stesso file (vedi `comando.json.esempio`), commit e push: la giornata.
3. Freno d'emergenza: "Cancel workflow" nella pagina della run; la pulizia e il controllo finale girano lo stesso.
   Se resta qualcosa: `{"modo": "smonta"}`.

Durante la giornata il log stampa ogni minuto la riga `SPESA` (stima per eccesso) e l'ultimo passo dei generatori; a
5 $ scrive `BLOCCO` e pulisce. L'ultimo passo, sempre: "RIMASTO: niente" oppure ALLARME e job rosso. I risultati
finiscono in `proto/quaderno/results/q8-aws-<run>/` sul branch (senza il numero dell'account): `gen/corsie/corsie.jsonl`
(una riga per fase, come in locale) e `r0|r1|r2/campioni.jsonl` (CPU, memoria e disco di ogni replica ogni 5 s, da
abbinare alle fasi con `t_start` e `t_end`).

## File

- `../../../.github/workflows/q8-corsie.yml`: il job (copiato dalla struttura di M31).
- `giornata.sh`: bucket, quattro macchine, attesa con la spesa, BLOCCO, risultati.
- `macchina.sh`: quello che fa ogni macchina (replica o generatori). `campiona.py`: CPU, memoria, disco delle repliche.
- `permessi.sh`, `pulizia.sh`, `recupera.sh`, `controllo-finale.sh`: come M31.

## Cosa non è stato provato

Il job non è mai partito (niente AWS in questa sessione). La parte "remota" di `corsie.py` (`Q8_REMOTE=1`) è
provata in locale contro un gruppo acceso a mano; `macchina.sh` e `giornata.sh` solo con `bash -n`. Il primo giro
utile è il modo `permessi` (0 $).
