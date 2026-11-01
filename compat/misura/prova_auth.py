#!/usr/bin/env python3
"""Authentication with --auth: localhost exception, SCRAM-SHA-256 and SCRAM-SHA-1 login, wrong password, roles.
Usage: prova_auth.py <host:port>"""
import json
import sys

from pymongo import MongoClient
from pymongo.errors import OperationFailure

hp = sys.argv[1] if len(sys.argv) > 1 else "localhost:27020"
out = {}


def step(name, fn):
    try:
        fn()
        out[name] = "ok"
    except Exception as e:
        out[name] = "ERRORE: " + str(e)[:200]


def first_user():
    c = MongoClient(f"mongodb://{hp}/", directConnection=True)
    c.admin.command("createUser", "root", pwd="root-pw-1", roles=["root"])


def scram(mech):
    def f():
        c = MongoClient(f"mongodb://root:root-pw-1@{hp}/?authSource=admin&authMechanism={mech}", directConnection=True)
        c.admin.command("ping")
        c.app.t.insert_one({"a": 1})
    return f


def wrong_pw():
    c = MongoClient(f"mongodb://root:sbagliata@{hp}/?authSource=admin", directConnection=True,
                    serverSelectionTimeoutMS=3000)
    try:
        c.admin.command("ping")
    except OperationFailure as e:
        if e.code == 18:
            return
        raise
    raise AssertionError("wrong password accepted")


def no_login():
    c = MongoClient(f"mongodb://{hp}/", directConnection=True)
    try:
        c.app.t.find_one()
    except OperationFailure as e:
        if e.code == 13:
            return
        raise
    raise AssertionError("read without login accepted")


def read_role():
    r = MongoClient(f"mongodb://root:root-pw-1@{hp}/?authSource=admin", directConnection=True)
    r.app.command("createUser", "lettore", pwd="lettore-pw", roles=["read"])
    c = MongoClient(f"mongodb://lettore:lettore-pw@{hp}/?authSource=app", directConnection=True)
    c.app.t.find_one()
    try:
        c.app.t.insert_one({"b": 1})
    except OperationFailure as e:
        if e.code == 13:
            return
        raise
    raise AssertionError("write with role read accepted")


step("primo utente (localhost exception)", first_user)
step("login SCRAM-SHA-256", scram("SCRAM-SHA-256"))
step("login SCRAM-SHA-1", scram("SCRAM-SHA-1"))
step("password sbagliata: errore 18", wrong_pw)
step("senza login: errore 13", no_login)
step("ruolo read: legge, non scrive", read_role)
print(json.dumps({"driver": "pymongo (autenticazione, motore con --auth)", "steps": out}, ensure_ascii=False))
