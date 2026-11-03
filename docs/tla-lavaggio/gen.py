#!/usr/bin/env python3
"""Genera i file .cfg dei modelli di LavaggioPerCliente (vedi PROPRIETA.md).

Uso: python3 gen.py   (scrive i .cfg in questa cartella)
"""
import os

BASE = dict(
    Clients='{A, B}', Colls='{"k"}', Vals="{1, 2}",
    MaxSeq=4, MaxBoxes=5, Workers='{"w1"}', Readers='{"r1"}',
    Cap=1, MinSources=2, Grace=2, Hold=1, RHold=1, FHold=1, MaxForks=1, MaxCrash=1, MaxErase=1,
    NoInputCheck="FALSE", BlindPut="FALSE", DropTombAlways="FALSE", SplitSwap="FALSE",
    ReaderTwoSnap="FALSE", ForkNoLedger="FALSE", GcNoPins="FALSE", GcOnlyRemoved="FALSE",
    Track="FALSE",
)

SAFETY = ["NessunDatoPerso", "FoglioIntegro", "IndicePuntaDoc", "DocHaIndice", "UnaVersione",
          "LettoreMaiSenzaScatola", "RamoCorretto", "RamoIntegro"]
LIVENESS = ["NessunOrfanoPerSempre", "PileSottoIlTetto", "CancellazioneFinita"]
SANITY = ["MaiLavaggioFallitoPerCancellazione", "MaiRamoDopoCancellazione", "MaiCrashConOrfano",
          "MaiOrfanoPulito", "MaiLettoreDuranteScambio", "MaiCancellazioneFinita", "MaiLapideTolta",
          "MaiRicreatoDopoCancellazione", "MaiScambioConLettore"]

# componenti: lettori (R), cancellazione (E), ramo (F), lavaggi in parallelo (W)
LETTORI = dict(MaxErase=0, MaxForks=0, MaxSeq=3)
CANCELLA = dict(Readers="{}", MaxSeq=3)
DUE = dict(Readers="{}", MaxForks=0, Workers='{"w1", "w2"}', MaxBoxes=6, MaxSeq=3)
COLLEZIONI = dict(Colls='{"k1", "k2"}', Vals="{1}", MaxForks=0, MaxBoxes=6, MaxSeq=3)
TUTTO = dict(MaxSeq=3)
VIVO = dict(Vals="{1}", MaxSeq=3, Readers="{}", MaxForks=0, MaxBoxes=4)
VIVO_RAMO = dict(VIVO, MaxForks=1, MaxErase=0, MaxBoxes=5)
SOLO = dict(Readers="{}", MaxForks=0, MaxErase=0)   # base dei negativi: si accende solo il pezzo che serve

MODELS = {
    # modelli giusti, sicurezza
    "lettori": (LETTORI, SAFETY, []),
    "cancella": (CANCELLA, SAFETY, []),
    "due-lavaggi": (DUE, SAFETY, []),
    "collezioni": (COLLEZIONI, SAFETY, []),
    "tutto": (TUTTO, SAFETY, []),
    # vivezza
    "vivo": (VIVO, [], LIVENESS),
    "vivo-ramo": (VIVO_RAMO, [], LIVENESS),
    # raggiungibilita': ogni invariante deve essere violata (si gira con -continue)
    # (una corsa per caso: TLC si ferma al primo esempio trovato)
    # controlli negativi: ognuno sul modello piu' piccolo dove la regola tolta conta
    "neg-noinputcheck": (dict(SOLO, MaxSeq=3, MaxErase=1, NoInputCheck="TRUE"), SAFETY, []),
    "neg-blindput": (dict(SOLO, Vals="{1}", MaxSeq=3, BlindPut="TRUE"), SAFETY, []),
    "neg-droptomb": (dict(SOLO, MaxSeq=3, DropTombAlways="TRUE"), SAFETY, []),
    "neg-splitswap": (dict(SOLO, MaxSeq=3, SplitSwap="TRUE", MaxBoxes=6), SAFETY, []),
    "neg-readertwosnap": (dict(SOLO, Readers='{"r1"}', MaxSeq=3, ReaderTwoSnap="TRUE"), SAFETY, []),
    "neg-forknoledger": (dict(SOLO, Vals="{1}", MaxSeq=3, MaxErase=1, MaxForks=1, ForkNoLedger="TRUE"), SAFETY, []),
    "neg-gcnopins": (dict(SOLO, Vals="{1}", MaxSeq=3, MaxForks=1, GcNoPins="TRUE"), SAFETY, []),
    "neg-grace0": (dict(SOLO, Readers='{"r1"}', Vals="{1}", MaxSeq=3, Grace=0, Hold=0, RHold=1, FHold=0), SAFETY, []),
    "neg-gconlyremoved": (dict(VIVO, GcOnlyRemoved="TRUE"), [], LIVENESS),
    "neg-minsources3": (dict(VIVO, MinSources=3), [], LIVENESS),
}


def write(name, over, invs, props):
    c = dict(BASE)
    c.update(over)
    lines = ["\\* generato da gen.py: modello " + name, "CONSTANTS"]
    for k, v in c.items():
        lines.append(f"  {k} = {v}")
    lines.append("SPECIFICATION " + ("LiveSpec" if props else "Spec"))
    if not props:
        lines.append("SYMMETRY SymClients")
    if invs:
        lines.append("INVARIANTS")
        lines += ["  " + i for i in invs]
    if props:
        lines.append("PROPERTIES")
        lines += ["  " + p for p in props]
    lines.append("CHECK_DEADLOCK FALSE" if props else "CHECK_DEADLOCK TRUE")
    with open(os.path.join(os.path.dirname(__file__) or ".", f"LavaggioPerCliente-{name}.cfg"), "w") as f:
        f.write("\n".join(lines) + "\n")


# ogni caso sul modello piu' piccolo dove puo' succedere (stesso codice, pezzi spenti)
RAGG = {
    "MaiCancellazioneFinita": TUTTO, "MaiCrashConOrfano": TUTTO, "MaiLapideTolta": TUTTO,
    "MaiLavaggioFallitoPerCancellazione": dict(SOLO, MaxSeq=3, MaxErase=1),
    "MaiLettoreDuranteScambio": dict(SOLO, MaxSeq=3, Readers='{"r1"}'),
    "MaiScambioConLettore": dict(SOLO, MaxSeq=3, Readers='{"r1"}'),
    "MaiOrfanoPulito": dict(SOLO, MaxSeq=3),
    "MaiRamoDopoCancellazione": dict(SOLO, Vals="{1}", MaxSeq=3, MaxForks=1, MaxErase=1),
    "MaiRicreatoDopoCancellazione": dict(SOLO, Vals="{1}", MaxSeq=3, MaxErase=1),
}
for _inv in SANITY:
    MODELS["ragg-" + _inv[3:]] = (dict(RAGG[_inv], Track="TRUE"), [_inv], [])

if __name__ == "__main__":
    for n, (o, i, p) in MODELS.items():
        write(n, o, i, p)
    print("scritti", len(MODELS), "modelli")
