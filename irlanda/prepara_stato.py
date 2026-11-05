"""P1 Irlanda v2 · prepara il file di stato PRIMA del giro (sul runner, 0 $, nessuna connessione). Lo script non cambia.
Perche': il file di stato tiene un solo 'giro_finale'. Dopo il giro M30 dentro c'e' parallelo_livelli_fatti = L1, L10,
L50, L100: lo script crederebbe i livelli 10 e 50 gia' fatti e li salterebbe (MISURATO a secco: "Parallelo: livelli
gia' finiti ... L10, L50", 0 clienti nel parallelo). Qui il giro di prima si sposta sotto la chiave
'giro_finale_prima_di_irlanda' dello stesso file (come le due righe di PASSO-7b per M10). Non tocca 'usati',
'usati_grandi', 'clienti', 'grandi' ne' il registro dei clienti letti.
Uso: prepara_stato.py FILE_STATO   (scrittura in un colpo con os.replace; codice 1 = FERMO, file non toccato)"""
import json
import os
import sys

CHIAVE = "giro_finale_prima_di_irlanda"


def main():
    f = sys.argv[1]
    d = json.load(open(f))
    if CHIAVE in d:
        print(f"FERMO: '{CHIAVE}' c'e' gia': questo file e' gia' passato da un giro dall'Irlanda. Servono i file "
              f"di stato giusti (quelli del Mac dopo il giro M30)")
        sys.exit(1)
    gf = d.get("giro_finale")
    if not gf:
        print("FERMO: nel file di stato non c'e' 'giro_finale': non sembra il file di DOPO il giro M30")
        sys.exit(1)
    d[CHIAVE] = d.pop("giro_finale")
    with open(f + ".tmp", "w") as o:
        json.dump(d, o, indent=1)
    os.replace(f + ".tmp", f)
    print(f"OK    giro di prima ('{gf.get('nome')}', livelli del parallelo fatti "
          f"{', '.join(gf.get('parallelo_livelli_fatti', [])) or 'nessuno'}) spostato sotto '{CHIAVE}'; "
          f"usati {len(d.get('usati', []))}, usati da 2 KB {len(d.get('usati_grandi', []))} (non toccati)")


if __name__ == "__main__":
    main()
