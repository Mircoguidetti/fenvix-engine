"""Item 3: TLS. Each case runs against real mongod 7.0.45 and the engine started on the same certificates (tls_env.py)."""
import os
from gaps_py import case, CERTS
from pymongo import MongoClient
from pymongo.errors import OperationFailure, PyMongoError

CA = "ca.pem"


def cert(n):
    return os.path.join(CERTS, n)


def mk(client, host=None, **opts):
    h, p = client.gaps_addr
    opts = {k: (cert(v) if isinstance(v, str) and v.endswith(".pem") else v) for k, v in opts.items()}
    return MongoClient(host or h, p, serverSelectionTimeoutMS=3000, **opts)


def run(f):
    try:
        r = f()
        return "ok" if r is None else r
    except OperationFailure as e:
        return {"code": e.code, "codeName": (e.details or {}).get("codeName")}
    except PyMongoError as e:
        return {"exc": type(e).__name__}


def ping(c):
    return run(lambda: c.admin.command("ping")["ok"])


@case("g03.tls.crud_over_tls", pair="A", opts={"tls": True, "tlsCAFile": CA})
def _t1(client, db):
    t = db.t
    t.insert_many([{"_id": i, "v": "x" * 1000} for i in range(50)])
    big = {"_id": "big", "blob": "y" * (9 * 1024 * 1024)}
    t.insert_one(big)
    t.update_one({"_id": 3}, {"$set": {"z": 1}})
    return {"n": t.count_documents({}), "doc": t.find_one({"_id": 3})["z"], "big": len(t.find_one({"_id": "big"})["blob"]), "agg": list(t.aggregate([{"$group": {"_id": None, "n": {"$sum": 1}}}]))}


@case("g03.tls.uri_options", pair="A")
def _t2(client, db):
    h, p = client.gaps_addr
    ca = cert(CA)
    out = {}
    for label, q in {"tls": f"tls=true&tlsCAFile={ca}", "ssl_alias": f"ssl=true&tlsCAFile={ca}", "insecure": "tls=true&tlsInsecure=true", "allowInvalid": "tls=true&tlsAllowInvalidCertificates=true", "no_ca": "tls=true", "plain": "tls=false"}.items():
        out[label] = ping(MongoClient(f"mongodb://{h}:{p}/?{q}", serverSelectionTimeoutMS=3000))
    return out


@case("g03.tls.wrong_ca_and_plain_client", pair="A")
def _t3(client, db):
    return {"other_ca": ping(mk(client, tls=True, tlsCAFile="other-ca.pem")), "plain": ping(mk(client)), "allow_invalid": ping(mk(client, tls=True, tlsAllowInvalidCertificates=True)), "insecure": ping(mk(client, tls=True, tlsInsecure=True))}


@case("g03.tls.hostname_verification", pair="A")
def _t4(client, db):
    return {"ip_in_san": ping(mk(client, host="127.0.0.1", tls=True, tlsCAFile=CA)), "san_name": ping(mk(client, host="node1.example.test", tls=True, tlsCAFile=CA)),
            "not_in_san": ping(mk(client, host="node9.example.test", tls=True, tlsCAFile=CA)), "not_in_san_allowed": ping(mk(client, host="node9.example.test", tls=True, tlsCAFile=CA, tlsAllowInvalidHostnames=True))}


@case("g03.tls.optional_client_certificate", pair="A")
def _t5(client, db):
    return {"no_cert": ping(mk(client, tls=True, tlsCAFile=CA)), "with_cert": ping(mk(client, tls=True, tlsCAFile=CA, tlsCertificateKeyFile="client.pem")),
            "untrusted_cert": ping(mk(client, tls=True, tlsCAFile=CA, tlsCertificateKeyFile="untrusted-client.pem"))}


@case("g03.tls.self_signed_server_certificate", pair="B")
def _t6(client, db):
    c = mk(client, tls=True, tlsCAFile="selfsigned.pem")
    db2 = c[db.name]
    db2.t.insert_one({"_id": 1})
    return {"trusted": ping(c), "find": list(db2.t.find()), "wrong_ca": ping(mk(client, tls=True, tlsCAFile=CA)), "plain": ping(mk(client)), "insecure": ping(mk(client, tls=True, tlsInsecure=True))}


@case("g03.tls.client_certificate_required", pair="C")
def _t7(client, db):
    return {"no_cert": ping(mk(client, tls=True, tlsCAFile=CA)), "client": ping(mk(client, tls=True, tlsCAFile=CA, tlsCertificateKeyFile="client.pem")),
            "client2": ping(mk(client, tls=True, tlsCAFile=CA, tlsCertificateKeyFile="client2.pem")), "untrusted": ping(mk(client, tls=True, tlsCAFile=CA, tlsCertificateKeyFile="untrusted-client.pem")),
            "plain": ping(mk(client))}


@case("g03.tls.preferTLS_accepts_both", pair="D")
def _t8(client, db):
    p = mk(client)
    t = mk(client, tls=True, tlsCAFile=CA)
    p[db.name].t.insert_one({"_id": 1, "via": "plain"})
    t[db.name].t.insert_one({"_id": 2, "via": "tls"})
    return {"plain": ping(p), "tls": ping(t), "both_see_both": [sorted(d["_id"] for d in x[db.name].t.find()) for x in (p, t)]}


# ---- MONGODB-X509 (pair E: client certificate required, --auth)
SUBJECT = "CN=client1,OU=Dev,O=Clients,C=US"


@case("g03.x509.authenticate_with_client_certificate", pair="E")
def _x1(client, db):
    root = mk(client, tls=True, tlsCAFile=CA, tlsCertificateKeyFile="client.pem", username="root", password="rootpw", authSource="admin")
    out = {}
    root["$external"].command("createUser", SUBJECT, roles=[{"role": "readWrite", "db": db.name}])
    x = mk(client, tls=True, tlsCAFile=CA, tlsCertificateKeyFile="client.pem", authMechanism="MONGODB-X509", authSource="$external")
    out["status"] = x.admin.command("connectionStatus")["authInfo"]["authenticatedUsers"]
    out["own_db_write"] = run(lambda: x[db.name].t.insert_one({"_id": 1}) and None)
    out["other_db_write"] = run(lambda: x[db.name + "_o"].t.insert_one({"_id": 1}) and None)
    out["no_user_cert"] = run(lambda: mk(client, tls=True, tlsCAFile=CA, tlsCertificateKeyFile="client2.pem", authMechanism="MONGODB-X509", authSource="$external").admin.command("ping") and None)
    out["explicit_user"] = run(lambda: mk(client, tls=True, tlsCAFile=CA, tlsCertificateKeyFile="client.pem", authMechanism="MONGODB-X509", authSource="$external", username=SUBJECT).admin.command("ping") and None)
    out["wrong_user"] = run(lambda: mk(client, tls=True, tlsCAFile=CA, tlsCertificateKeyFile="client.pem", authMechanism="MONGODB-X509", authSource="$external", username="CN=someone").admin.command("ping") and None)
    out["unauthenticated_cert_only"] = run(lambda: mk(client, tls=True, tlsCAFile=CA, tlsCertificateKeyFile="client.pem")[db.name].t.find_one())
    root["$external"].command("dropUser", SUBJECT)
    out["after_drop"] = run(lambda: mk(client, tls=True, tlsCAFile=CA, tlsCertificateKeyFile="client.pem", authMechanism="MONGODB-X509", authSource="$external").admin.command("ping") and None)
    return out


@case("g03.tls.scram_over_tls_with_auth", pair="E")
def _x2(client, db):
    root = mk(client, tls=True, tlsCAFile=CA, tlsCertificateKeyFile="client.pem", username="root", password="rootpw", authSource="admin")
    root[db.name].command("createUser", "tu", pwd="pw", roles=[{"role": "read", "db": db.name}])
    c = mk(client, tls=True, tlsCAFile=CA, tlsCertificateKeyFile="client.pem", username="tu", password="pw", authSource=db.name)
    out = {"find": run(lambda: list(c[db.name].t.find()) and None), "insert": run(lambda: c[db.name].t.insert_one({"a": 1}) and None)}
    root[db.name].command("dropAllUsersFromDatabase")
    return out


# ---- mongodb+srv:// (names served by the local DNS server; the engine's part is TLS with a certificate for node1.example.test)
import dns.resolver

_r = dns.resolver.Resolver(configure=False)
_r.nameservers = ["127.0.0.1"]
_r.port = 5353
_r.lifetime = 4
dns.resolver.default_resolver = _r

PAIR_OF_PORT = {27040: "areal", 27048: "aours", 27041: "breal", 27049: "bours", 27042: "creal", 27050: "cours", 27043: "dreal", 27051: "dours", 27044: "ereal", 27052: "eours"}


def srv_name(client, prefix=None):
    n = PAIR_OF_PORT[client.gaps_addr[1]]
    return f"{prefix}{n[-4:] if n.endswith('real') else n[-4:]}" if prefix else n


def srv(client, q="", name=None, creds=""):
    name = name or srv_name(client)
    return MongoClient(f"mongodb+srv://{creds}{name}.example.test/?{q}", serverSelectionTimeoutMS=4000)


@case("g03.srv.basic_and_defaults", pair="A")
def _s1(client, db):
    ca = f"tlsCAFile={cert(CA)}"
    c = srv(client, ca)
    c[db.name].t.insert_one({"_id": 1})
    return {"find": list(c[db.name].t.find()), "nodes": sorted(h for h, _ in c.nodes), "tls_default_on": ping(srv(client, "")), "tls_off": ping(srv(client, "tls=false")), "ssl_off": ping(srv(client, "ssl=false"))}


@case("g03.srv.preferTLS_server_with_tls_false", pair="D")
def _s2(client, db):
    return {"tls": ping(srv(client, f"tlsCAFile={cert(CA)}")), "plain": ping(srv(client, "tls=false"))}


@case("g03.srv.txt_record_supplies_authSource", pair="E")
def _s4(client, db):
    root = mk(client, tls=True, tlsCAFile=CA, tlsCertificateKeyFile="client.pem", username="root", password="rootpw", authSource="admin")
    root[db.name].command("createUser", "su", pwd="pw", roles=[{"role": "read", "db": db.name}])
    q = f"tlsCAFile={cert(CA)}&tlsCertificateKeyFile={cert('client.pem')}"
    out = {"root_authSource_from_txt": run(lambda: srv(client, q, creds="root:rootpw@").admin.command("ping") and None),
           "user_in_other_db_needs_override": run(lambda: srv(client, q + f"&authSource={db.name}", creds="su:pw@").admin.command("ping") and None),
           "user_default_source_from_txt": run(lambda: srv(client, q, creds="su:pw@").admin.command("ping") and None)}
    root[db.name].command("dropAllUsersFromDatabase")
    return out


@case("g03.srv.errors", pair="A")
def _s5(client, db):
    ca = f"tlsCAFile={cert(CA)}"
    real = client.gaps_addr[1] == 27040
    return {"no_such_name": run(lambda: MongoClient(f"mongodb+srv://nosuch.example.test/?{ca}", serverSelectionTimeoutMS=3000).admin.command("ping")),
            "outside_parent_domain": run(lambda: MongoClient(f"mongodb+srv://outside.example.test/?{ca}", serverSelectionTimeoutMS=3000).admin.command("ping")),
            "txt_disallowed_option": run(lambda: MongoClient(f"mongodb+srv://badtxt{'real' if real else 'ours'}.example.test/?{ca}", serverSelectionTimeoutMS=3000).admin.command("ping")),
            "srvServiceName": ping(MongoClient(f"mongodb+srv://custom{'real' if real else 'ours'}.example.test/?{ca}&srvServiceName=custom", serverSelectionTimeoutMS=3000)),
            "srvMaxHosts_with_replicaSet": run(lambda: MongoClient(f"mongodb+srv://multi{'real' if real else 'ours'}.example.test/?{ca}&srvMaxHosts=1&replicaSet=x"))}
