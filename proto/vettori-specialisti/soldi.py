"""M23, punto 5: i prezzi DICHIARATI dei servizi gestiti di ricerca vettoriale contro il nostro costo (regole di M20).

Uso: python3 soldi.py
Clienti di B10/B18: medio 50 milioni di vettori, grande 500 milioni, 1.024 dimensioni (4 KB in float), 30 giorni.
Prezzi (letti il 2026-11-03, tutti DICHIARATI dalla fonte scritta accanto):
- Pinecone serverless: storage $0,33/GB-mese, lettura $16 per milione di RU (Standard); una domanda costa 1 RU per GB
  del namespace, minimo 0,25 (pinecone.io/pricing; Blocks & Files 1/12/2025).
- Pinecone Dedicated Read Nodes: b1 $0,46/ora ($336,42/mese) in aws-us-east-1, 250 GB per shard, costo =
  nodo x shard x repliche + storage (docs.pinecone.io, DRN overview e concepts). Ricerche/s per nodo: NON SO.
- turbopuffer: storage fino a $0,33/GB-mese, domande $1 per PB letto, sconto 80% tra 32 e 128 GB per domanda e 96%
  sopra 128 GB, minimo 1,28 GB per domanda (usagepricing.com, fatti controllati il 22/10/2026). Spazi "fissati" a ore:
  prezzo NON SO (non pubblico).
- S3 Vectors (us-east-1): storage $0,06/GB-mese, $2,50 per milione di domande, dati letti per domanda $0,004/TB per i
  primi 100.000 vettori, $0,002/TB fino a 10 milioni, $0,0004/TB oltre (pagina prezzi S3, citata da developersdigest).
- Weaviate Cloud: "da" $0,00465 (Flex), $0,003875 (Premium condiviso), $0,002718 (Premium dedicato) per milione di
  dimensioni al mese; il "da" vale per la compressione più forte (weaviate.io/pricing). Minimo Flex $45, Premium da $400.
- Qdrant Cloud: prezzi non pubblicati sul sito (solo calcolatore). Terza parte: $0,078 per GB di RAM all'ora con
  i vCPU compresi (LeanOps, 4/4/2026). Fragile.
Il nostro costo: ours_best di M20 (proto/cuochi-vettori/soldi.py), con R = rapporto di CPU contro lo specializzato
(misurato qui) al posto del rapporto contro Lucene. Prezzo = costo / 0,55.
"""
import math, os, sys

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "cuochi-vettori"))
from soldi import CLIENTI, ours_best  # noqa: E402

H = 730.0
SEC = 30 * 86400
CL = {"medio": dict(n=50e6, qps=[50, 250]), "grande": dict(n=500e6, qps=[500, 2500])}


def gb(n):
    return n * 4096 / 1e9


def pinecone_serverless(n, qps):
    g = gb(n)
    return g * 0.33 + qps * SEC * max(0.25, g) * 16e-6


def pinecone_drn(n, replicas=2):
    g = gb(n)
    shards = math.ceil(g / 250)
    return shards * replicas * 336.42 + g * 0.33, shards * replicas


def turbopuffer(n, qps):
    g = gb(n)
    eq = min(g, 32) + 0.2 * max(0, min(g, 128) - 32) + 0.04 * max(0, g - 128)
    eq = max(eq, 1.28)
    return g * 0.33 + qps * SEC * eq * 1e-6  # $1/PB = $1e-6/GB


def s3_vectors(n, qps):
    v = 4096 / 1e12  # TB per vettore
    per_q = (min(n, 1e5) * v * 0.004 + max(0, min(n, 1e7) - 1e5) * v * 0.002 + max(0, n - 1e7) * v * 0.0004
             + 2.5e-6)
    return gb(n) * 0.06 + qps * SEC * per_q


def weaviate(n):
    mdims = n * 1024 / 1e6
    return mdims * 0.002718, mdims * 0.00465


def qdrant_cloud(ram_gb, replicas=2):
    return ram_gb * 0.078 * H * replicas


def main():
    R = float(sys.argv[1]) if len(sys.argv) > 1 else 1.0
    print(f"R (nostra CPU contro il migliore specializzato) = {R}")
    for name, c in CL.items():
        n = c["n"]
        drn, nodes = pinecone_drn(n)
        w_lo, w_hi = weaviate(n)
        for qps in c["qps"]:
            print(f"\n== {name}: {n/1e6:.0f} milioni di vettori ({gb(n):.0f} GB in float), {qps} ricerche/s ==")
            print(f"Pinecone serverless ${pinecone_serverless(n, qps):,.0f}")
            print(f"Pinecone DRN (b1, 2 repliche, {nodes} nodi; capienza in ricerche/s NON SO) ${drn:,.0f}")
            print(f"turbopuffer a consumo ${turbopuffer(n, qps):,.0f} (fissato: NON SO)")
            print(f"S3 Vectors ${s3_vectors(n, qps):,.0f}")
            print(f"Weaviate Cloud (prezzo 'da', compressione forte) ${w_lo:,.0f}–{w_hi:,.0f}")
            for fam in ("int8", "binario"):
                cl = CLIENTI[f"{name} {fam}"]
                q = qdrant_cloud(cl["ram_o"])
                # la regola di M20 dimensiona noi con 10 x R ricerche/s per vCPU, R contro Lucene; qui contro lo
                # specializzato: Atlas e gli specializzati non c'entrano col nostro costo, che dipende dalla nostra CPU.
                o = ours_best(cl, qps, 13.48 if fam == "int8" else 5.17)
                print(f"  {fam}: Qdrant Cloud (RAM {cl['ram_o']:.0f} GB x 2, terza parte) ${q:,.0f}; "
                      f"noi costo ${o[0]:,.0f} ({o[1]}), prezzo ${o[0]/0.55:,.0f}")


if __name__ == "__main__":
    main()
