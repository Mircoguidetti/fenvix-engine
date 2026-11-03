"""Trasforma le traiettorie pubbliche di tau2-bench (Sierra Research, data/tau2/results/final)
in eventi di lettura e scrittura su un database, come se ogni strumento dell'agente fosse
una query MongoDB.

Ogni strumento del banco è marcato READ o WRITE nel codice di tau2 (src/tau2/domains/*/tools.py).
Qui sotto la traduzione in collezioni e gettoni (chiavi lette o toccate). Dove non si sa quale
documento cambia, si butta tutta la famiglia ("|*"): è la scelta prudente, sfavorevole al maître.

Ambiti (chi condivide il maître):
  condiviso: tutte le conversazioni dello stesso file (un modello, un dominio, 4 tentativi per
             compito, girate in parallelo) su un solo database, con i tempi veri registrati;
  compito:   solo i tentativi dello stesso compito si vedono (il caso "tanti tentativi" di Berkeley);
  sessione:  ogni conversazione da sola (un agente, un cliente).
  paralleli: come "compito", ma i tentativi dello stesso compito partono tutti insieme
             (come i "field agents" in parallelo dello studio di Berkeley).
  clienti:   solo il primo tentativo di ogni compito, tutti sullo stesso database con i tempi
             veri: tanti clienti diversi, un agente ciascuno. È il più vicino a un'app vera.
"""
import json
from datetime import datetime


def ts(s):
    return datetime.fromisoformat(s).timestamp()


def jload(s):
    try:
        return json.loads(s)
    except Exception:
        return None


def flights_in(obj, out):
    """Raccoglie (numero volo, data) da una risposta qualunque."""
    if isinstance(obj, dict):
        fn = obj.get("flight_number")
        dt = obj.get("date")
        if fn and dt:
            out.add(f"{fn}/{dt}")
        for v in obj.values():
            flights_in(v, out)
    elif isinstance(obj, list):
        for v in obj:
            flights_in(v, out)
    return out


def read_spec(dom, name, a, res):
    """-> (coll, toks) o None se lo strumento non legge il database."""
    if dom == "retail":
        if name == "find_user_id_by_name_zip":
            return "users", [f"users|namezip|{a.get('first_name')}/{a.get('last_name')}/{a.get('zip')}"]
        if name == "find_user_id_by_email":
            return "users", [f"users|email|{a.get('email')}"]
        if name == "get_user_details":
            return "users", [f"users|id|{a.get('user_id')}"]
        if name == "get_order_details":
            return "orders", [f"orders|id|{a.get('order_id')}"]
        if name == "get_product_details":
            return "products", [f"products|id|{a.get('product_id')}"]
        if name == "get_item_details":
            return "products", [f"products|item|{a.get('item_id')}"]
        if name == "list_all_product_types":
            return "products", ["products|all|all"]
    if dom == "airline":
        if name == "get_reservation_details":
            return "reservations", [f"reservations|id|{a.get('reservation_id')}"]
        if name == "get_user_details":
            return "users", [f"users|id|{a.get('user_id')}"]
        if name == "list_all_airports":
            return "airports", ["airports|all|all"]
        if name in ("search_direct_flight", "search_onestop_flight"):
            toks = [f"flights|route|{a.get('origin')}-{a.get('destination')}-{a.get('date')}"]
            toks += [f"flights|fd|{f}" for f in sorted(flights_in(jload(res), set()))]
            return "flights", toks
        if name == "get_flight_status":
            return "flights", [f"flights|fd|{a.get('flight_number')}/{a.get('date')}"]
    if dom == "telecom":
        if name == "get_customer_by_phone":
            r = jload(res) or {}
            toks = [f"customers|phone|{a.get('phone_number')}"]
            if isinstance(r, dict) and r.get("customer_id"):
                toks.append(f"customers|id|{r['customer_id']}")
            return "customers", toks
        if name == "get_customer_by_id":
            return "customers", [f"customers|id|{a.get('customer_id')}"]
        if name == "get_customer_by_name":
            r = jload(res)
            toks = [f"customers|name|{a.get('full_name')}/{a.get('dob')}"]
            if isinstance(r, list):
                toks += [f"customers|id|{x.get('customer_id')}" for x in r if isinstance(x, dict)]
            return "customers", toks
        if name == "get_details_by_id":
            i = str(a.get("id"))
            coll = {"L": "lines", "D": "devices", "B": "bills", "P": "plans"}.get(i[:1], "customers")
            return coll, [f"{coll}|id|{i}"]
        if name == "get_bills_for_customer":
            return "bills", [f"bills|cust|{a.get('customer_id')}"]
        if name == "get_data_usage":
            return "lines", [f"lines|id|{a.get('line_id')}"]
    return None


def write_spec(dom, name, a, res):
    """-> (colls, toks) o None se lo strumento non scrive nel database."""
    r = jload(res)
    if dom == "retail":
        oid = a.get("order_id")
        uid = r.get("user_id") if isinstance(r, dict) else None
        utok = [f"users|id|{uid}"] if uid else ["users|id|*"]
        if name == "cancel_pending_order" or name in ("modify_pending_order_items", "modify_pending_order_payment",
                                                      "return_delivered_order_items", "exchange_delivered_order_items"):
            return ["orders", "users"], [f"orders|id|{oid}"] + utok
        if name == "modify_pending_order_address":
            return ["orders"], [f"orders|id|{oid}"]
        if name == "modify_user_address":
            return ["users"], [f"users|id|{a.get('user_id')}", "users|namezip|*"]
    if dom == "airline":
        rid = a.get("reservation_id")
        uid = (r.get("user_id") if isinstance(r, dict) else None) or a.get("user_id")
        utok = [f"users|id|{uid}"] if uid else ["users|id|*"]
        if name == "book_reservation":
            fl = {f"{f.get('flight_number')}/{f.get('date')}" for f in (a.get("flights") or []) if isinstance(f, dict)}
            return ["reservations", "users", "flights"], utok + [f"flights|fd|{f}" for f in sorted(fl)]
        if name == "cancel_reservation":
            fl = flights_in(r, set())
            return ["reservations", "users", "flights"], [f"reservations|id|{rid}"] + utok + [f"flights|fd|{f}" for f in sorted(fl)]
        if name == "update_reservation_flights":
            # i voli vecchi non sono nella risposta: si butta tutta la famiglia dei voli
            return ["reservations", "users", "flights"], [f"reservations|id|{rid}"] + utok + ["flights|fd|*"]
        if name == "update_reservation_baggages":
            return ["reservations", "users"], [f"reservations|id|{rid}"] + utok
        if name == "update_reservation_passengers":
            return ["reservations"], [f"reservations|id|{rid}"]
        if name == "send_certificate":
            return ["users"], [f"users|id|{a.get('user_id')}"]
    if dom == "telecom":
        cid, lid = a.get("customer_id"), a.get("line_id")
        if name in ("suspend_line", "resume_line"):
            return ["lines", "customers"], [f"lines|id|{lid}", f"customers|id|{cid}"]
        if name in ("enable_roaming", "disable_roaming"):
            return ["lines"], [f"lines|id|{lid}"]
        if name == "refuel_data":
            return ["lines", "bills"], [f"lines|id|{lid}", f"bills|cust|{cid}"]
        if name == "send_payment_request":
            return ["bills"], [f"bills|id|{a.get('bill_id')}", f"bills|cust|{cid}"]
        if name == "make_payment":   # strumento del cliente che paga una bolletta
            return ["bills"], ["bills|id|*", "bills|cust|*"]
    return None


def events(path, scope):
    d = json.load(open(path))
    fname = path.rsplit("/", 1)[-1]
    dom = d["info"]["environment_info"]["domain_name"] if "info" in d and "environment_info" in d["info"] else None
    if dom is None:
        for x in ("airline", "retail", "telecom"):
            if f"_{x}" in fname:
                dom = x
    out = []
    stats = {"conversazioni": 0, "letture": 0, "scritture": 0, "scritture_fallite": 0, "altri": 0}
    t0_task = {}
    for s in d["simulations"]:
        t0_task[s["task_id"]] = min(t0_task.get(s["task_id"], 1e18), ts(s["start_time"]))
    for s in d["simulations"]:
        if scope == "clienti" and s.get("trial") != 0:
            continue
        stats["conversazioni"] += 1
        if scope in ("condiviso", "clienti"):
            db = fname
        elif scope in ("compito", "paralleli"):
            db = f"{fname}#{s['task_id']}"
        else:
            db = f"{fname}#{s['id']}"
        shift = (t0_task[s["task_id"]] - ts(s["start_time"])) if scope == "paralleli" else 0.0
        res = {m.get("id"): m for m in s["messages"] if m["role"] == "tool"}
        for m in s["messages"]:
            for tc in (m.get("tool_calls") or []):
                name, a = tc["name"], tc.get("arguments") or {}
                tm = res.get(tc.get("id"))
                content = tm["content"] if tm else ""
                err = bool(tm.get("error")) if tm else True
                t = ts((tm or m)["timestamp"]) + shift
                rs = read_spec(dom, name, a, content) if m["role"] == "assistant" else None
                if rs:
                    coll, toks = rs
                    qkey = name + json.dumps(a, sort_keys=True)
                    out.append({"t": t, "db": db, "coll": coll, "kind": "read", "qkey": qkey,
                                "shape": name, "toks": toks, "size": len(content.encode()), "grp": dom})
                    stats["letture"] += 1
                    continue
                ws = write_spec(dom, name, a, content)
                if ws:
                    if err:
                        stats["scritture_fallite"] += 1
                        continue
                    colls, toks = ws
                    out.append({"t": t, "db": db, "coll": colls[0], "colls": colls, "kind": "write", "toks": toks})
                    stats["scritture"] += 1
                    continue
                stats["altri"] += 1
    return out, stats, dom
