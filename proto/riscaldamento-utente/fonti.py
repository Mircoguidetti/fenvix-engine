"""M24 · dalle tracce pubbliche agli eventi (t, chi, oggetto, padrone, tipo) di sim.py."""
import glob
import gzip
import json
import os
from datetime import datetime


# ---------------------------------------------------------------- tau2-bench

def _ts(s):
    return datetime.fromisoformat(s).timestamp()


def _j(s):
    try:
        return json.loads(s)
    except Exception:
        return None


def tau2_events(path, domains_dir, login=False):
    """Un file di tau2 (un modello, un dominio). Solo il primo tentativo di ogni compito.

    Oggetti: users|U, orders|O, reservations|R (padrone dal db.json); prodotti e voli condivisi.
    find_user_id_by_* : indice sempre caldo (segnale puro) che rivela il cliente.
    Con login=True l'inizio della conversazione è un segnale del cliente (app con login).
    """
    fname = os.path.basename(path)
    dom = "retail" if "_retail_" in fname else "airline" if "_airline_" in fname else None
    if dom is None:
        return [], {}
    db = json.load(open(os.path.join(domains_dir, dom, "db.json")))
    own = {}
    if dom == "retail":
        for oid, o in db["orders"].items():
            own[f"orders|{oid}"] = o["user_id"]
    else:
        for rid, r in db["reservations"].items():
            own[f"reservations|{rid}"] = r["user_id"]
    d = json.load(open(path))
    ev = []
    st = {"conversazioni": 0, "chiamate": 0, "letture_dati": 0, "senza_cliente": 0}
    for s in d["simulations"]:
        if s.get("trial") != 0:
            continue
        st["conversazioni"] += 1
        res = {m.get("id"): m for m in s["messages"] if m["role"] == "tool"}
        calls = []
        for m in s["messages"]:
            if m["role"] != "assistant":
                continue
            for tc in (m.get("tool_calls") or []):
                tm = res.get(tc.get("id"))
                t = _ts((tm or m)["timestamp"])
                calls.append((t, tc["name"], tc.get("arguments") or {}, (tm or {}).get("content", ""),
                              bool(tm.get("error")) if tm else True))
        calls.sort(key=lambda x: x[0])
        # chi è il cliente: il primo user_id che compare
        cust = None
        for t, name, a, content, err in calls:
            cand = a.get("user_id")
            if not cand and name.startswith("find_user_id") and not err:
                cand = (content or "").strip().strip('"') or None
            if not cand:
                r = _j(content)
                if isinstance(r, dict):
                    cand = r.get("user_id")
            if cand:
                cust = cand
                break
        if cust is None:
            st["senza_cliente"] += 1
            cust = f"anon#{s['id']}"
        known = login
        if login:
            ev.append((_ts(s["start_time"]), cust, None, cust, "s"))
        for t, name, a, content, err in calls:
            st["chiamate"] += 1
            obj, g, kind = None, None, None
            reveal = False
            if name.startswith("find_user_id"):
                kind, reveal = "s", True
            elif name == "get_user_details":
                obj, g, kind = f"users|{a.get('user_id')}", a.get("user_id"), "r"
                reveal = True
            elif name == "get_order_details":
                obj = f"orders|{a.get('order_id')}"
                g, kind = own.get(obj), "r"
            elif name == "get_reservation_details":
                obj = f"reservations|{a.get('reservation_id')}"
                g, kind = own.get(obj), "r"
            elif name in ("get_product_details",):
                obj, kind = f"products|{a.get('product_id')}", "r"
            elif name == "get_item_details":
                obj, kind = f"items|{a.get('item_id')}", "r"
            elif name in ("list_all_product_types", "list_all_airports"):
                obj, kind = f"catalog|{name}", "r"
            elif name in ("search_direct_flight", "search_onestop_flight"):
                obj, kind = f"flights|{a.get('origin')}-{a.get('destination')}-{a.get('date')}", "r"
            elif name == "get_flight_status":
                obj, kind = f"flightstatus|{a.get('flight_number')}/{a.get('date')}", "r"
            elif name in ("cancel_pending_order", "modify_pending_order_items", "modify_pending_order_payment",
                          "return_delivered_order_items", "exchange_delivered_order_items",
                          "modify_pending_order_address"):
                if err:
                    continue
                obj = f"orders|{a.get('order_id')}"
                g, kind = own.get(obj), "w"
            elif name in ("cancel_reservation", "update_reservation_flights", "update_reservation_baggages",
                          "update_reservation_passengers"):
                if err:
                    continue
                obj = f"reservations|{a.get('reservation_id')}"
                g, kind = own.get(obj), "w"
            elif name in ("modify_user_address", "send_certificate"):
                if err:
                    continue
                obj, g, kind = f"users|{a.get('user_id')}", a.get("user_id"), "w"
            elif name == "book_reservation":
                if err:
                    continue
                r = _j(content) or {}
                rid = r.get("reservation_id") if isinstance(r, dict) else None
                obj, g, kind = f"reservations|{rid}", a.get("user_id"), "w"
                if rid:
                    own[obj] = a.get("user_id")
            else:
                continue
            if err and kind == "r":
                # lettura di un oggetto che non esiste: è comunque una richiesta (segnale)
                obj, kind = None, "s"
            who = cust if known else None
            if reveal:
                known = True
                who = cust
            if kind == "r":
                st["letture_dati"] += 1
            ev.append((t, who, obj, g if obj is not None else (cust if kind == "s" else None), kind))
    ev.sort(key=lambda e: e[0])
    return ev, st


def tau2_group_size(domains_dir, dom):
    db = json.load(open(os.path.join(domains_dir, dom, "db.json")))
    n = {}
    for u in db["users"]:
        n[u] = 1
    key = "orders" if dom == "retail" else "reservations"
    for x in db[key].values():
        n[x["user_id"]] = n.get(x["user_id"], 0) + 1
    return lambda g, t: n.get(g, 1), n


# ---------------------------------------------------------------- Twitter

WRITE_OPS = {"set", "add", "replace", "cas", "append", "prepend", "incr", "decr", "delete"}


def twitter_events(path, app_cache=60.0, max_rows=None):
    """Campione di Twitter: chi = client id (servizio frontend, non una persona). Nessun padrone.
    Prima una cache dell'app di 60 s (come M9 e M15): le letture ripetute entro 60 s non arrivano."""
    ev = []
    lastread = {}
    n = 0
    with open(path) as f:
        for line in f:
            p = line.rstrip("\n").split(",")
            if len(p) < 7:
                continue
            n += 1
            if max_rows and n > max_rows:
                break
            t = float(p[0])
            key, client, op = p[1], p[4], p[5]
            if op in ("get", "gets"):
                lr = lastread.get(key)
                if lr is not None and t - lr <= app_cache:
                    continue
                lastread[key] = t
                ev.append((t, client, key, None, "r"))
            elif op in WRITE_OPS:
                ev.append((t, client, key, None, "w"))
    return ev


# ---------------------------------------------------------------- GH Archive

AREA = {
    "PushEvent": "code", "CreateEvent": "code", "DeleteEvent": "code", "ReleaseEvent": "code",
    "GollumEvent": "wiki",
    "IssuesEvent": "issues", "IssueCommentEvent": "issues",
    "PullRequestEvent": "pulls", "PullRequestReviewEvent": "pulls", "PullRequestReviewCommentEvent": "pulls",
    "CommitCommentEvent": "pulls",
    "WatchEvent": "meta", "ForkEvent": "meta", "PublicEvent": "meta", "MemberEvent": "meta",
}


def gh_events(path, sampled_owner=None, with_bots=False):
    """Ogni evento = lettura dello stato di un pezzo di repository (repo#area), fatta da actor.
    Padrone = la parte del nome prima della '/'. Con sampled_owner si tengono come letture solo
    gli oggetti di padroni nel campione (gli altri restano segnali dei loro actor)."""
    ev = []
    with gzip.open(path, "rt") as f:
        for line in f:
            p = line.rstrip("\n").split("\t")
            if len(p) < 4:
                continue
            t, actor, repo, typ = int(p[0]), p[1], p[2], p[3]
            if not with_bots and (actor.endswith("[bot]") or actor.endswith("-bot")):
                continue
            owner = repo.split("/", 1)[0]
            area = AREA.get(typ, "other")
            if sampled_owner is not None and not sampled_owner(owner):
                ev.append((t, actor, None, None, "s"))
                continue
            ev.append((t, actor, f"{repo}#{area}", owner, "r"))
    ev.sort(key=lambda e: e[0])
    return ev


# ---------------------------------------------------------------- REES46 (e-commerce)

def rees46_events(path):
    """REES46, negozio multi-categoria, ottobre 2019 (10% degli utenti, ora al secondo).
    view     -> lettura del prodotto (catalogo condiviso), richiesta dell'utente
    cart     -> lettura del carrello dell'utente (cart|U, suo)
    purchase -> lettura dello storico ordini dell'utente (orders|U, suo); il carrello letto nella stessa
                richiesta non conta due volte (sarebbe un segnale finto, nello stesso istante)
    """
    from datetime import timezone
    ev = []
    gsize = {}
    objs = set()
    with gzip.open(path, "rt") as f:
        for line in f:
            p = line.rstrip("\n").split("\t")
            if len(p) < 4:
                continue
            t = datetime.strptime(p[0][:19], "%Y-%m-%d %H:%M:%S").replace(tzinfo=timezone.utc).timestamp()
            u, typ, pid = p[1], p[2], p[3]
            if typ == "view":
                o = f"p|{pid}"
                ev.append((t, u, o, None, "r"))
                objs.add(o)
            elif typ == "cart":
                ev.append((t, u, f"cart|{u}", u, "r"))
                objs.add(f"cart|{u}")
            elif typ == "purchase":
                ev.append((t, u, f"orders|{u}", u, "r"))
                objs.add(f"orders|{u}")
            gsize[u] = 2
    ev.sort(key=lambda e: e[0])
    return ev, {"eventi": len(ev), "utenti": len(gsize), "oggetti": len(objs), "_gsize": gsize,
                "giorni": round((ev[-1][0] - ev[0][0]) / 86400, 2)}
