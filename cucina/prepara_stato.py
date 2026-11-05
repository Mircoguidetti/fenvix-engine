"""Cucina minima (K2; copia di P1 Irlanda prepara_stato.py, con il nome della chiave come argomento) · sulla macchina
cliente, PRIMA del giro di ogni taglia dopo la prima (0 $, nessuna connessione). Lo script non cambia.
Perche': il file di stato tiene un solo 'giro_finale'. Dopo il giro della taglia di prima dentro c'e'
parallelo_livelli_fatti = L10, L50: lo script crederebbe i livelli gia' fatti e li salterebbe (lo stesso problema
dell'Irlanda dopo M30, MISURATO a secco nel brain). Qui il giro di prima si sposta sotto la chiave
'giro_finale_prima_di_<NOME>' dello stesso file. Non tocca 'usati', 'clienti' ne' il registro dei clienti letti.
Senza 'giro_finale' (prima taglia, subito dopo il caricamento) non fa niente.
Uso: prepara_stato.py FILE_STATO NOME   (scrittura in un colpo con os.replace; codice 1 = FERMO, file non toccato)"""
import json
import os
import sys


def main():
    f, nome = sys.argv[1], sys.argv[2]
    chiave = f"giro_finale_prima_di_{nome}"
    d = json.load(open(f))
    if chiave in d:
        print(f"FERMO: '{chiave}' c'e' gia': questa taglia ha gia' girato su questo file di stato")
        sys.exit(1)
    gf = d.get("giro_finale")
    if not gf:
        print(f"OK    nessun giro di prima nel file di stato (prima taglia): niente da spostare; usati {len(d.get('usati', []))}")
        return
    d[chiave] = d.pop("giro_finale")
    with open(f + ".tmp", "w") as o:
        json.dump(d, o, indent=1)
    os.replace(f + ".tmp", f)
    print(f"OK    giro di prima ('{gf.get('nome')}', livelli del parallelo fatti "
          f"{', '.join(gf.get('parallelo_livelli_fatti', [])) or 'nessuno'}) spostato sotto '{chiave}'; "
          f"usati {len(d.get('usati', []))} (non toccati)")


if __name__ == "__main__":
    main()
