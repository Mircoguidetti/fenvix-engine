# M26 · Intermedio 1 (lavori in corso)

Sessione M26 (Opus), branch `lavaggio-per-cliente` di `fenvix-engine`, cartella `proto/lavaggio-per-cliente/`. Soglie scritte prima in `SOGLIE.md`. $0, tutto in locale.

## Dove sono

- Fatto: carico misto (chat, eventi, ordini con aggiornamenti; 3 collezioni per cliente, indice globale), tre disposizioni delle chiavi (oggi, collezione, cliente in testa), strategie (a)(b)(c) con SlateDB 0.17, e (d) "livelli geometrici" con uno schedulatore nostro (SlateDB permette di inserirlo, senza toccare la libreria). Simulatore del lavaggio sui metadati, tarato sulle misure.
- Prime misure vere a 0,25 GB con scrittore a ritmo fisso (2 MB/s): amplificazione del lavaggio (byte scritti dal lavaggio / byte dell'utente) 3,9 senza tetto, 7,3 livelli T=4, 11,1 tetto a 4 pile, 14,0 tetto a 2 pile (MISURATO). La disposizione "oggi" scrive come "cliente" (3,95 contro 3,91, MISURATO): raggruppare non costa lavaggio in più.
- La regola "una GET per pila dove c'è il cliente" regge anche con 3 collezioni: scaldare un cliente con la disposizione "cliente in testa" costa una GET per pila; con la disposizione di oggi 162 GET in media (MISURATO).
- Simulatore a 20 TB (STIMATO): il tetto di SlateDB a 2–6 pile riscrive tutto il database a ogni lavaggio piccolo (amplificazione da 10.000 a 150.000): impossibile. Senza tetto: amplificazione circa 13, pile circa 12. Livelli T=4: amplificazione circa 29, pile circa 6–8.

In corso: 1 e 2 GB, verifica del simulatore, conti dei soldi. Rapporto finale a seguire.
