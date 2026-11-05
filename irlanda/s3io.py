"""P1 Irlanda · S3 dalla macchina, con il ruolo della macchina (boto3, credenziali da IMDS). Niente chiavi.
  s3io.py sali BUCKET CARTELLA PREFISSO        carica i file nuovi o cambiati (taglia o data) della cartella
  s3io.py scarica BUCKET CHIAVE DESTINAZIONE   scarica un oggetto
  s3io.py prendi-e-cancella BUCKET CHIAVE      stampa su stdout il contenuto (senza a capo finale) e cancella l'oggetto
AWS_ENDPOINT_URL (solo la prova locale) punta a un S3 finto."""
import json
import os
import sys

import boto3


def cliente():
    return boto3.client("s3", region_name=os.environ.get("AWS_REGION", "eu-west-1"))


def sali(bucket, cartella, prefisso):
    s3 = cliente()
    memo_p = os.path.join(cartella, ".salito.json")
    try:
        memo = json.load(open(memo_p))
    except Exception:
        memo = {}
    for radice, _, nomi in os.walk(cartella):
        for n in nomi:
            p = os.path.join(radice, n)
            if p == memo_p:
                continue
            rel = os.path.relpath(p, cartella)
            try:
                st = os.stat(p)
            except OSError:
                continue
            firma = [st.st_size, st.st_mtime_ns]
            if memo.get(rel) == firma:
                continue
            try:
                s3.upload_file(p, bucket, prefisso + rel)
                memo[rel] = firma
            except Exception as e:  # si riprova al giro dopo
                print(f"sali {rel}: {type(e).__name__}", file=sys.stderr)
    with open(memo_p + ".tmp", "w") as f:
        json.dump(memo, f)
    os.replace(memo_p + ".tmp", memo_p)


def main():
    cmd, bucket = sys.argv[1], sys.argv[2]
    if cmd == "sali":
        sali(bucket, sys.argv[3], sys.argv[4])
    elif cmd == "scarica":
        os.makedirs(os.path.dirname(os.path.abspath(sys.argv[4])), exist_ok=True)
        cliente().download_file(bucket, sys.argv[3], sys.argv[4])
    elif cmd == "prendi-e-cancella":
        s3 = cliente()
        v = s3.get_object(Bucket=bucket, Key=sys.argv[3])["Body"].read().decode().strip()
        s3.delete_object(Bucket=bucket, Key=sys.argv[3])
        sys.stdout.write(v)
    else:
        sys.exit(f"comando sconosciuto: {cmd}")


if __name__ == "__main__":
    main()
