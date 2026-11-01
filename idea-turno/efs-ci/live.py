#!/usr/bin/env python3
"""Scrive idea-turno/efs-ci/LIVE.md (letto dal telefono del fondatore; il coordinatore/S13 lo copia nel brain).
Uso: live.py fine <modo>   (a fine run, dai file in $OUT). La prova lo aggiorna da sola con scrivi_live()."""
import os, sys, time
OUT = os.environ.get("OUT", "risultati-efs")
LIVE = "idea-turno/efs-ci/LIVE.md"

def leggi(n):
    try: return open(os.path.join(OUT, n)).read().strip()
    except Exception: return ""

def scrivi_live(spesa, fase, extra="", allarme=""):
    t = time.strftime("%Y-%m-%d %H:%M UTC", time.gmtime())
    s = ""
    if allarme: s += f"# ALLARME: {allarme}\n\n"
    s += f"# Micro-prova EFS (S13), live\n\nAggiornato: {t}. Run GitHub: {os.environ.get('GITHUB_RUN_ID','?')}.\n\n"
    s += f"{spesa}\n\nFase in corso: {fase}\n"
    if extra: s += f"\n{extra}\n"
    open(LIVE, "w").write(s)

if __name__ == "__main__" and sys.argv[1:2] == ["fine"]:
    modo = sys.argv[2] if len(sys.argv) > 2 else "?"
    pul = leggi("pulizia.txt").splitlines()
    ultima_pul = next((l for l in reversed(pul) if l.startswith(("RIMASTO", "ALLARME", "NON SO"))), "pulizia: NON SO (non è girata)")
    spese = [l for l in leggi("spesa.log").splitlines() if l.startswith("SPESA")]
    spesa = spese[-1] if spese else "SPESA: $0,00 finora (stima), tetto $15 (nessuna risorsa a pagamento accesa)"
    extra = f"Modo: {modo}.\n\n{leggi('esito.txt')}\n\nPulizia: {ultima_pul}\n"
    m = leggi("mancanti.txt")
    if m: extra += "\nPermessi mancanti (azioni IAM da aggiungere al ruolo):\n" + "\n".join("- " + l for l in m.splitlines()) + "\n"
    allarme = ultima_pul if ultima_pul.startswith("ALLARME") else ""
    scrivi_live(spesa, "finito", extra, allarme)
