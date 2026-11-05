# Q8c su AWS: il quaderno condiviso "come Atlas" con zone e dischi veri

Pronta da lanciare, **non lanciata**. La autorizza il fondatore (spesa su AWS). Workflow:
`.github/workflows/q8c-quaderno.yml` (branch `quaderno-come-atlas`).

## Cosa fa

La stessa prova fatta in locale da `scripts/q8c.py`, su macchine vere in eu-west-1:

- una cella con 4 quaderni (registri), ognuno con tre copie, una per zona (a, b, c): ogni macchina delle copie tiene
  una copia di ogni quaderno, sul suo disco gp3 (fdatasync vero su EBS). Il capo preferito del quaderno g è la macchina
  g % 3: i capi si spargono sulle tre macchine;
- una macchina dei generatori nella zona a: i database normali a 100 scritture al secondo, i vicini rumorosi, gli
  scaricatori finti (uno per quaderno, senza S3);
- livelli 1 (tre volte), 2, 20, 50, 100 database; le sette fasi (nessuno, 1.000/s, 20.000/s, raffiche da 50.000, record
  da 1 MB, vicino che muore e riparte, il 10% rumorosi insieme); 60 s di misura per fase (120 s con 1–2 normali);
- configurazione del quaderno uguale alla prova locale Q8c: corsie, riciclo dei segmenti (con la correzione di Q8c),
  la porta per database (1.000 record/s e 2 MB/s, 0,1 s di scorta, al più 0,5 s in fila fuori, oltre un rifiuto subito con il tempo d'attesa giusto, deciso prima di leggere il resto del messaggio),
  finestra 10 s; in più la replica compressa (zstd 1).
- CPU per core ogni secondo su tutte e quattro le macchine (`campiona.py` sulle copie, `q8c.py` sui generatori). Una
  fase vale solo se nessun core di nessuna macchina è sopra il 90% in media e il generatore dei normali ha mandato
  almeno il 98% del chiesto. `scripts/q8c_analyze.py q8c.jsonl --replicas <cartella dei risultati>` fa il conto.

## Macchine e quota

- La quota di vCPU on-demand standard oggi è 8 (NOW.md, 2026-11-05). Con 8: quattro macchine da 2 vCPU
  (`TIPO_REPLICA` e `TIPO_GEN` nel workflow, oggi `c7i.large`). Rischio: con 2 vCPU la macchina dei generatori potrebbe
  essere il limite ai livelli 50 e 100 con i vicini (in locale lo è già): quelle fasi verrebbero NON VALIDE e si scrive.
- Con la quota a 16 (richiesta aperta da AWS): `c7i.xlarge` per tutte e quattro (4 vCPU), `PREZZO_ORA` 0,1915.
- Il modo `permessi` controlla anche la quota: scrive "QUOTA vCPU: Q, serve S".

## Quanto costa (STIMATO)

- Macchine: 4 × c7i.large a circa 0,096 $/h (STIMATO: metà del prezzo DICHIARATO di c7i.xlarge, 0,1915 $/h, doit.com
  ripreso da M31) per circa 80 minuti: circa 0,5 $. Con 4 × c7i.xlarge: circa 1,0 $.
- Traffico tra zone: circa 13 milioni di record da 2,2 KB (i vicini ora sono tenuti a 1.000 record/s dalla porta), al
  più 4 attraversamenti ciascuno, 0,02 $ per GB attraversato: al più circa 2,3 $ (con zstd probabilmente circa 1,2 $).
- Dischi, IPv4, S3: meno di 0,10 $.
- Totale: circa 1,5–3 $ (con c7i.xlarge circa 2–3,5 $). Tetto: 5 $ (si ferma da solo e pulisce).

## Prima di lanciare (fondatore, console IAM, una volta)

1. Ruolo GitHub `fenvix-gha-tests`, trust policy: aggiungere
   `repo:Mircoguidetti/fenvix-engine:ref:refs/heads/quaderno-come-atlas` alla condizione su
   `token.actions.githubusercontent.com:sub`.
2. Policy inline `fenvix-coldread-test`: permettere `ec2:RunInstances` del tipo scelto (`c7i.large`, oppure
   `c7i.xlarge` con la quota a 16). NON SO se oggi c'è: il modo `permessi` lo dice con `--dry-run` a 0 $.
3. Per la riga della quota nel modo `permessi`: `servicequotas:GetServiceQuota` e `ec2:DescribeInstanceTypes` (solo
   lettura). Senza, il controllo scrive NON SO e va avanti.

Nomi in AWS già autorizzati per M31 e Q8: tag `project=fenvix-coldread`, ruolo della macchina
`fenvix-coldread-machine`, bucket `fenvix-coldread-q8-<run>`.

## Come si lancia

1. `{"modo": "permessi"}` in `proto/quaderno/aws-q8c/comando.json`, commit e push sul branch `quaderno-come-atlas`:
   controllo a 0 $. Deve finire con "PERMESSI: tutti presenti".
2. `{"modo": "prova"}` nello stesso file, commit e push: la giornata.
3. Freno d'emergenza: "Cancel workflow" nella pagina della run; pulizia e controllo finale girano lo stesso. Se resta
   qualcosa: `{"modo": "smonta"}`.

Durante la giornata il log stampa ogni minuto la riga `SPESA` e l'ultimo passo dei generatori; a 5 $ scrive `BLOCCO` e
pulisce. Ultimo passo, sempre: "RIMASTO: niente" oppure ALLARME e job rosso. I risultati vanno in
`proto/quaderno/results/q8c-aws-<run>/` sul branch: `gen/q8c/q8c.jsonl` (una riga per fase) e
`r0|r1|r2/campioni.jsonl` (CPU per core, processi e disco di ogni macchina delle copie, ogni secondo).

## Cosa non è stato provato

Il job non è mai partito (niente AWS in questa sessione). `macchina.sh`, `giornata.sh`, `permessi.sh` provati solo con
`bash -n`; `q8c.py` in modo remoto (`Q8_REMOTE=1`) non provato contro macchine vere.
