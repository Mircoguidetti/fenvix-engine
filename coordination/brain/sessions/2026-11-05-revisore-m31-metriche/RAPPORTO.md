APPROVATO CON CORREZIONI

# Revisore · M31 scarico delle metriche CloudWatch (modo "metriche")

Revisore Opus, 2026-11-05. Ho letto il lavoro della sessione `sessions/2026-11-05-m31-metriche/RAPPORTO.md` (valido: prima riga PRONTO, file fermo dalle 10:05Z) e il commit `dd8ffff` sul branch `freddo-s3` di fenvix-engine (workflow `m31-freddo-s3.yml` e cartella `proto/lavaggio-per-cliente/m31/giornata/`). Nessuna chiamata AWS, nessuna spesa, nessun commit sui repo di codice.

In una riga: il modo nuovo fa solo letture, non tocca i modi vecchi e non può far partire una giornata a pagamento. Nessun errore provato. Una correzione da fare nel comando.json (la finestra di tempo scritta a mano, per non dipendere dall'ora del commit) e una consigliata nel workflow (copia di riserva se il push non riesce).

## Il verdetto, nella scala di RULES.md

Viva, con correzioni. Probabilità che il modo non scarichi le metriche della giornata, se si fa la correzione 1 e si incolla la policy: circa 10% (STIMATO), quasi tutta per il formato vero delle risposte di AWS che nessuno ha ancora visto (vedi "Rischi").

## Cosa ho controllato, un punto alla volta

1. **Solo letture.** Nel cuoco nuovo (metriche.sh) le sole chiamate ad AWS sono `ec2 describe-instances`, `ec2 describe-volumes`, `cloudwatch list-metrics`, `cloudwatch get-metric-data`. Nessuna crea, cambia o cancella. Nel workflow, in modo metriche, saltano `permessi.sh` (che crea e cancella un bucket di prova) e `pulizia.sh` (che cancella); girano solo il rinnovo delle credenziali (nessuna risorsa) e `controllo-finale.sh`, che ho letto: guarda e basta. L'ho anche provato: nel registro delle chiamate dell'aws finto ci sono solo quelle quattro (MISURATO, scenario mio "macchina sparita", 9 chiamate tutte di lettura).

2. **I modi vecchi.** Il diff del workflow cambia tre righe: il `case` accetta anche "metriche", il controllo dei permessi e la pulizia hanno in più la condizione "non in modo metriche". Per permessi, prova e smonta le condizioni restano vere come prima. L'unico cambio di comportamento è voluto dal prompt: `permessi.sh` ora controlla anche i due permessi CloudWatch, quindi permessi e prova si fermano con "MANCA" finché la policy non è incollata. La sessione lo ha scritto in chiaro.

3. **Un push non può far partire una giornata a pagamento.** Tre cancelli, tutti al loro posto:
   - il workflow parte solo per un push che cambia `comando.json` (filtro `paths`); il modo metriche pubblica solo dentro `risultati/aws-<run>/cloudwatch/`;
   - il push del modo metriche usa il gettone di GitHub Actions, e un push fatto con quel gettone non fa partire altri workflow (DICHIARATO, documentazione GitHub "Triggering a workflow from a workflow");
   - il primo passo trasforma in "smonta" qualsiasi comando.json non cambiato nell'ultimo commit o con un modo sconosciuto.
   Ho rifatto a mano il primo passo con 8 comando.json (MISURATO): metriche con run in testo e in numero → metriche; run con caratteri strani (`$(touch x)`) → restano solo le cifre, nessun comando eseguito; senza run → metriche con run vuota (poi metriche.sh esce con errore e niente pulizia); file non cambiato nel commit, "Metriche" con la maiuscola, JSON rotto → smonta, come prima.

4. **La macchina si trova anche dopo la cancellazione.** Sì: la prima scelta dopo il comando.json è il campo `id` di `risultati/aws-41999425773/giornata.json`, che è già sul branch (commit `d988e6a`) e vale `i-01ab8d98e0d4aa711` (MISURATO, letto dal file). Questo non scade. Ho provato lo scenario "macchina non più visibile, ID solo da giornata.json" con l'aws finto: macchina trovata, 17 metriche della macchina e 11 del disco scaricate, codice 0 (MISURATO). Quello che scade non è la macchina ma **la finestra di tempo e il disco**: vedi correzione 1.

5. **Nessun numero di account nei file.** metriche.sh sostituisce l'account con `<ACCOUNT_ID>` in tutti i file (anche nei messaggi d'errore di AWS dentro note.txt) e, se ne restasse uno, cancella la cartella e non pubblica; il workflow ricontrolla prima del commit. Se l'account mancasse nell'ambiente, il controllo del workflow con una stringa vuota trova "qualcosa" in ogni file e quindi non pubblica: sbaglia dalla parte sicura. Nelle prove (le sue 8 e la mia) nessun numero di account nei file (MISURATO).

6. **La policy.** Ho confrontato con un programma il JSON del rapporto con quello del prompt: i 9 blocchi di oggi sono identici, carattere per carattere dopo la lettura del JSON, e c'è un solo blocco in più, `ReadCloudWatchMetrics`, con soltanto `cloudwatch:GetMetricData` e `cloudwatch:ListMetrics` su `"Resource": "*"` (MISURATO). CloudWatch non ha nomi di risorsa per le metriche, quindi `"*"` è l'unica scelta (DICHIARATO, documentazione IAM di CloudWatch).

7. **permessi.sh controlla i due permessi con letture vere.** Le due righe nuove sono `cloudwatch list-metrics` (una metrica al massimo) e `cloudwatch get-metric-data` (una metrica, ultimi 10 minuti). Nessuna scrive. Provato con un aws finto mio: con tutto permesso scrive due OK e finisce bene; con CloudWatch negato scrive due MANCA e finisce con errore (MISURATO).

## Le prove della sessione, rifatte

- actionlint 1.7.12 sul workflow: nessun errore di sintassi; 20 avvisi, tutti di shellcheck sullo stile (18 "metti le virgolette", 2 "variabile i non usata" nel ciclo dei tre tentativi, uno nel passo nuovo e uno uguale in quello vecchio) (MISURATO). Il rapporto diceva lo stesso.
- shellcheck 0.11.0, livello warning, su metriche.sh, permessi.sh, prova-metriche.sh: pulito (MISURATO).
- prova-metriche.sh con l'aws finto: 66 controlli OK, 0 falliti (MISURATO), uguale al rapporto.
- controlla-policy.py: 0 controlli falliti (MISURATO).
- Mie in più: primo passo del workflow con 8 comando.json; scenario "macchina sparita, ID da giornata.json, nessuna finestra nel comando"; permessi.sh con CloudWatch permesso e negato. Tutte come atteso (MISURATO).

## Le correzioni, elenco esatto

**Correzione 1 (da fare, nel comando.json, non nel codice): scrivere la finestra a mano.**
La macchina è stata cancellata verso le 10:12Z (STIMATO: ultima riga di passi.log "fine" alle 10:11:52Z, risultati pubblicati alle 10:13Z). Resta visibile in DescribeInstances circa un'ora (DICHIARATO da AWS), quindi fino alle 11:12Z circa, cioè le 12:12 italiane. Dopo:
- la finestra automatica diventa "ultime 6 ore fino ad adesso": va ancora bene fino alle 15:05Z (16:05 italiane); dopo perde l'inizio della giornata (partita alle 09:05Z);
- il disco (volume EBS) si trova solo dalle metriche "attive nelle ultime 3 ore": verso le 13:12Z (14:12 italiane) sparisce e si scaricano solo le metriche della macchina (STIMATO, dal codice: `--recently-active PT3H`).
Le ore vere della giornata sono già nei risultati: macchina pronta alle 09:10:16Z, fine alle 10:11:52Z (MISURATO, passi.log). Il comando.json da committare è quindi:

```json
{"modo": "metriche", "run": "41999425773", "inizio": "2026-11-05T09:00:00Z", "fine": "2026-11-05T10:30:00Z"}
```

E va committato **prima delle 14:00 ora italiana di oggi**, se si vogliono anche le metriche del disco. Le metriche del disco viste dalla macchina (EBSReadBytes, EBSWriteOps e simili, nel gruppo AWS/EC2) arrivano comunque, a qualsiasi ora.

**Correzione 2 (consigliata, nel workflow, non blocca): copia di riserva se il push non riesce.**
Nel passo "Metriche CloudWatch…", se i tre tentativi di push falliscono, il passo esce con il codice di metriche.sh (0) e finisce verde: i file sono persi con la macchina virtuale di GitHub. Il modo prova ha già la copia come artifact; il modo metriche no. Due righe:
- in fondo al passo, prima di `exit $ok`: `git fetch -q origin freddo-s3 && git diff --quiet HEAD origin/freddo-s3 -- $R/cloudwatch || { echo "push non riuscito"; ok=1; }`;
- un passo nuovo subito dopo: `uses: actions/upload-artifact@v4`, `if: always() && steps.mode.outputs.modo == 'metriche'`, `name: m31-metriche-${{ github.run_id }}`, `path: proto/lavaggio-per-cliente/m31/risultati/aws-${{ steps.mode.outputs.run }}/cloudwatch`, `if-no-files-found: ignore`, `retention-days: 7`.

**Correzione 3 (piccola, facoltativa): numero di run troppo corto.**
Il primo passo tiene solo le cifre: un run scritto male (per esempio "4199$(x)" diventa "4199") finisce in una cartella `aws-4199` che non ha giornata.json, e la macchina si cerca col tag nelle ultime 24 ore. Innocuo (sola lettura, cartella col nome sbagliato), ma basta in metriche.sh `[[ "$RUN" =~ ^[0-9]{8,}$ ]]` al posto di `^[0-9]+$`.

Nota per il coordinatore, non è una correzione: per rifare il modo (per esempio dopo un "MANCA ListMetrics" perché la policy non era ancora incollata) il nuovo comando.json deve essere **diverso** dal precedente, altrimenti git non vede il cambio e non parte niente. Basta cambiare di un minuto "fine".

## Rischi (opinioni, non errori provati)

- **Il formato vero delle risposte di AWS non è stato visto da nessuno.** Il caso peggiore probabile: per la macchina cancellata la descrizione non porta più i dischi; c'è già la seconda strada. Con la correzione 1 l'ora di cancellazione non serve più. Se qualcosa va storto, il modo scrive note.txt e si può rifare a $0 di rischio (solo letture). Prova che decide: la run vera del modo metriche.
- **Dischi di altre macchine.** La seconda strada per i dischi tiene tutti i dischi cancellati nelle ultime 3 ore nell'account, non solo il nostro. Se nello stesso account e nella stessa regione qualcun altro ha cancellato dischi in quelle ore, si scaricano anche le loro metriche: sola lettura, spesa in più minima, ma nel file andrebbero distinti. Oggi l'unico altro uso dell'account in eu-west-1 è NON SO.
- **Spesa non proprio zero.** GetMetricData si paga 0,01 $ ogni 1.000 metriche chieste e non è nel livello gratuito (DICHIARATO, pagina prezzi CloudWatch, come scritto dalla sessione). Con circa 28 metriche per 5 statistiche per 2 periodi fanno circa 280 metriche chieste, circa 0,003 $ (STIMATO). Il prompt diceva "$0": la cifra va detta al fondatore insieme al comando, come vuole la regola sulle spese, anche se è meno di un centesimo.
- **Serie spezzate.** Con la finestra della correzione 1 (90 minuti, circa 90 punti per serie al minuto, STIMATO) GetMetricData sta in una pagina sola; con la finestra di 6 ore la risposta si spezza in più pagine e la stessa serie compare in due pezzi (la sessione lo ha già scritto). Altro motivo per la correzione 1.

## Errori o mancanze nel brain

- Il rapporto della sessione dice "Nessuna chiamata AWS vera, nessuna spesa" per la sessione stessa (giusto), ma il prompt M31-metriche chiama il modo "$0": costa circa 0,003 $ (STIMATO), vedi sopra.
- Nessun altro errore trovato.

## Link

- fenvix-engine, branch `freddo-s3`, commit `dd8ffff` (codice) e `d988e6a` (risultati della giornata con giornata.json).
- Rapporto della sessione: `sessions/2026-11-05-m31-metriche/RAPPORTO.md`.
