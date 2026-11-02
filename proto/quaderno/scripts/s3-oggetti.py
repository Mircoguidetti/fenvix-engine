#!/usr/bin/env python3
"""prova-zone: oggetti della copia del quaderno su S3 locale (moto o MinIO, porta 9000).
  s3-oggetti.py elenca                 inizio (indice) e dimensione di ogni oggetto della catena
  s3-oggetti.py togli-nel-mezzo        cancella l'oggetto a metà della catena; stampa il suo inizio e la sua fine
"""
import json, struct, sys
import boto3
s3 = boto3.client('s3', endpoint_url='http://127.0.0.1:9000', aws_access_key_id='minioadmin',
                  aws_secret_access_key='minioadmin', region_name='us-east-1')
B, P = 'pq-sst', 'cella/quaderno/'


def objs():
    out, tok = [], None
    while True:
        kw = {'Bucket': B, 'Prefix': P}
        if tok:
            kw['ContinuationToken'] = tok
        r = s3.list_objects_v2(**kw)
        out += [(o['Key'], o['Size']) for o in r.get('Contents', [])]
        if not r.get('IsTruncated'):
            return sorted(out)
        tok = r['NextContinuationToken']


def head(k):
    b = s3.get_object(Bucket=B, Key=k, Range='bytes=0-59')['Body'].read()
    start, end, _, _, sp, ep = struct.unpack('<6Q', b[8:56])
    return start, end, sp, ep


cmd = sys.argv[1]
if cmd == 'elenca':
    for k, n in objs():
        s, e, sp, ep = head(k)
        print(json.dumps({'chiave': k, 'byte': n, 'inizio': s, 'fine': e}))
elif cmd == 'togli-nel-mezzo':
    o = objs()
    k = o[len(o) // 2][0]
    s, e, sp, ep = head(k)
    s3.delete_object(Bucket=B, Key=k)
    print(json.dumps({'tolto': k, 'inizio': s, 'fine': e, 'pos_inizio': sp, 'pos_fine': ep, 'oggetti_prima': len(o),
                      'oggetti_dopo_il_buco': len(o) - len(o) // 2 - 1}))
