# L1 · Il lavaggio che non finisce e le correzioni del revisore di K1 (2026-11-05)

Sei una sessione di lavoro (Opus). Leggi CLAUDE.md, RULES.md, NOW.md, PLAN-LAVAGGIO.md e in DECISIONS.md le voci del 2026-11-05.

## Da dove parti
- K1: sessions/2026-11-05-k1-server-per-script/RAPPORTO.md, branch server-per-script.
- Il suo revisore: sessions/2026-11-05-revisore-k1-server-per-script/RAPPORTO.md, sezioni 3, 5 e 8.

## Il lavoro, in ordine
1. Il lavaggio (compattazione) che non finisce nel suo tempo massimo di 30 s ("did not finish (L0 8)"), riparte da capo, ferma le scritture (circa 11 minuti, MISURATO dal revisore) e lascia i fogli scritti (SST) in dispensa. Prima la prova che decide se è di main o di K1 (stesso caricamento, due caricatori, con il binario di main a9f46d3, in locale). Poi la correzione: il lavaggio deve finire anche con database grandi (il principio di CLAUDE.md: deve reggere 1 TB), i lavori abbandonati non lasciano fogli, le scritture non si fermano. Test che lo provano.
2. E1 del revisore: hostInfo vero (CPU usabili e memoria della macchina, non del modello registrato), con un test.
3. E2 del revisore: le letture per aprire o riaprire un database e scaldare il bancone vanno contate come lavoro di sfondo, non sul primo ordine; con un test.
4. Le note M1, M2, M3 del revisore nel tuo rapporto.
5. Il giro piccolo dello script contro il server corretto, in locale: tutte le fasi "fatta".

## Limiti
- Spesa $0: niente AWS, niente Atlas, niente connettori personali, niente merge.
- Branch tuo: motore-correzioni, da server-per-script. Mai main.
- Lo script di Atlas prova_infinite.py non si tocca (md5 8396ca455557baaa4e3a9b1f881451f9).
- Codice nuovo in inglese; il codice italiano che tocchi lo converti. Rapporto in italiano.
- Un'altra sessione (T1, attrezzi del cameriere) lavora sul percorso delle letture su un altro branch: tocca solo quello che serve a te e scrivi nel rapporto i file toccati, per l'unione.

## Consegna
sessions/2026-11-05-motore-correzioni/RAPPORTO.md: prima riga PRONTO o NON PRONTO, con il motivo. Ogni numero con la sua etichetta. Niente tabelle, niente orari. Push su main di fenvix-brain come in sessions/README.md. Poi fai partire il tuo revisore come dice il messaggio di lancio. In chat solo "Rapporto pronto: …".
