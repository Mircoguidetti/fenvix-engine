# Correzioni del revisore M31 (C1–C7): prove locali, 0 $

- `guasti-c1-c2-c3.txt`: `giornata/prova-locale/prova-guasti.sh`. Scaricamento fallito e tar fallito (C1), macchina muta (C2),
  rinnovo delle credenziali fallito (C3). Moto al posto di AWS, la "macchina" è lo user-data eseguito in locale con un
  `shutdown` finto. 0 controlli falliti.
- `freno-c7.txt`: `giornata/prova-locale/prova-freno.sh`. Tetto delle GET abbassato a 300: `tempi` si ferma con il codice 97 ed
  `esegui.sh` salta alla fine.
- `giornata-*`: `giornata/prova-locale/prova-giornata-locale.sh`. Tutta la giornata in piccolo (0,3 GB e 0,1 GB, 20 clienti
  per gruppo, ritardo finto 25:0.6:90): permessi, pacco, giornata-ci.sh, recupera.sh, pulizia.sh, risultati senza account.
  I millisecondi servono solo a controllare il programma (moto è lento), non come previsione.
