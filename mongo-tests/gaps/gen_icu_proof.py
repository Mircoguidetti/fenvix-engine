#!/usr/bin/env python3
"""Proof that the collation data of the engine (icu_collator_data 2.4.0, CLDR 49) has no tailoring for the locales that mongod 7.0.45 (ICU 57,
CLDR 29) tailors: for each locale, sort the same strings with mongod, with the engine, and with the engine under `en` (which has no tailoring, so it is the root order).
When the engine's order for the locale equals its root order and differs from mongod's, the tailoring is missing from the data.
usage: gen_icu_proof.py > compat/gaps_status/icu_locale_proof.md"""
from pymongo import MongoClient
from itertools import product

LOCALES = ["lv", "se", "se@collation=search", "smn", "smn@collation=search", "kl", "kl@collation=search", "haw", "en_US_POSIX", "cs@collation=search", "de@collation=search", "sk@collation=search"]
LETTERS = list("abcdefghijklmnopqrstuvwxyzAEIOUYZ") + ["å", "ä", "ö", "ø", "æ", "á", "š", "č", "ž", "ā", "ī", "ū", "ʻ", "ß", "ä", "ü", "ö", "ch", "dž", "ll", "ng", "ŋ", "1", "-", "_", " ", "ʼ", "ʻa", "ae", "oe", "ij"]
STRINGS = sorted(set(LETTERS) | {a + b for a, b in product("aeiouyz", "aeiouyz")})
real = MongoClient("mongodb://localhost:27017")
ours = MongoClient("mongodb://localhost:27019")


def order(client, locale):
    db = client["icu_proof"]
    db.t.drop()
    db.t.insert_many([{"_id": i, "s": s} for i, s in enumerate(STRINGS)])
    return [d["s"] for d in db.t.find({}, collation={"locale": locale}).sort("s", 1)]


def first_diff(a, b):
    for i, (x, y) in enumerate(zip(a, b)):
        if x != y:
            return i, a[max(0, i - 2):i + 3], b[max(0, i - 2):i + 3]
    return None


print("# Collation locales without tailoring in the engine's data\n")
print("mongod 7.0.45 uses ICU 57 (CLDR 29); the engine uses `icu_collator` 2.4.0 with `icu_collator_data` 2.4.0 (generated with CLDR 49, Unicode 18).")
print("For each locale the same strings are sorted by mongod, by the engine, and by the engine under `en` (which has no tailoring, so it is the root order). `engine == engine(en)` and `engine != mongod` means the engine's data has no tailoring for the locale.\n")
print("| locale | engine == engine(en) | engine == mongod | first difference (mongod / engine) |\n|---|---|---|---|")
root = order(ours, "en")
for loc in LOCALES:
    r, o = order(real, loc), order(ours, loc)
    d = first_diff(r, o)
    print(f"| `{loc}` | {o == root} | {r == o} | {('position %d: `%s` / `%s`' % d) if d else 'none'} |")
print("\nStrings used:", " ".join(f"`{s}`" for s in STRINGS[:40]), "…")
