"""Which documents each tau2 tool call reads and writes.

A document is (table, id). A table of the tau2 database is a collection, a record is a document. The rules below
were read from the tau2-bench source (`src/tau2/domains/<domain>/tools.py`, commit of 2026-11-04); the comment on
each rule names what the code does. Tools that run on the end user's own phone (telecom user tools) are not the
database and never reach this module.

A written document carries tags so that a query read ("all cards of account X") can find it:
  owner:<customer>   the customer it belongs to
  acct:<account id>  the account it hangs from (banking)
A read is a list of (table, key): key is a document id, a tag, or "*" (the whole table).
"""
import glob
import json
import os
import re

TAU2 = os.environ.get("TAU2", "/home/user/data/tau2")

# ---------------------------------------------------------------- tool types from the tau2 source

_TYPES = None


def tool_types():
    global _TYPES
    if _TYPES is None:
        _TYPES = {}
        for f in glob.glob(f"{TAU2}/src/tau2/domains/*/*.py"):
            src = open(f).read()
            for m in re.finditer(r"@is_(?:discoverable_)?tool\(ToolType\.(\w+)[^)]*\)\s*\n\s*def (\w+)", src):
                _TYPES.setdefault(m.group(2), m.group(1))
    return _TYPES


def ttype(name):
    t = tool_types().get(name)
    if t:
        return t
    if re.match(r"(get|find|search|list|check)_", name or ""):
        return "READ"
    return "WRITE"


# calls that touch no data
IGNORE = {"calculate", "transfer_to_human_agents", "get_current_time", "list_discoverable_agent_tools",
          "list_discoverable_user_tools", "unlock_discoverable_agent_tool", "give_discoverable_user_tool",
          "rewrite_context", "think", "list_all_airports", "example_agent_tool_0000", "call_discoverable_user_tool"}
KB_TOOLS = {"KB_search", "grep", "KB_search_bm25", "KB_search_dense", "shell"}


def unwrap_call(name, args):
    """call_discoverable_agent_tool(agent_tool_name, arguments) -> (real name, real arguments)."""
    if name in ("call_discoverable_agent_tool",):
        n2 = args.get("agent_tool_name") or args.get("tool_name") or name
        a2 = args.get("arguments") or {}
        if isinstance(a2, str):
            try:
                a2 = json.loads(a2)
            except Exception:
                a2 = {}
        return n2, a2 if isinstance(a2, dict) else {}
    return name, args


# ---------------------------------------------------------------- the initial database

def load_db(dom):
    p = f"{TAU2}/data/tau2/domains/{dom}"
    if os.path.exists(f"{p}/db.json"):
        return json.load(open(f"{p}/db.json"))
    import tomllib
    return tomllib.load(open(f"{p}/db.toml", "rb"))


def _unwrap(tab):
    if isinstance(tab, dict) and "data" in tab and isinstance(tab["data"], (dict, list)):
        tab = tab["data"]
    if isinstance(tab, list):
        out = {}
        for r in tab:
            if isinstance(r, dict):
                rid = next((r[k] for k in r if k.endswith("_id") and isinstance(r[k], str)), None)
                if rid:
                    out[rid] = r
        return out
    return tab if isinstance(tab, dict) else {}


def _idlike(s):
    return isinstance(s, str) and 2 < len(s) < 48 and " " not in s and any(c.isdigit() for c in s)


def _walk(x, out):
    if isinstance(x, dict):
        for k, v in x.items():
            if _idlike(k):
                out.add(k)
            _walk(v, out)
    elif isinstance(x, list):
        for v in x:
            _walk(v, out)
    elif _idlike(x):
        out.add(x)


class DB:
    """Initial database of one domain: tables, id -> table, id -> owner (M25's generic ownership rule, rewritten:
    the users/customers table owns every id inside its records; records with user_id/customer_id are owned; an id
    referenced by records of one owner only is owned by that owner; everything else is shared)."""

    def __init__(self, dom):
        self.dom = dom
        db = load_db(dom)
        self.tables = {k: _unwrap(v) for k, v in db.items()}
        if dom == "banking_knowledge":
            # banking tasks add their own users, accounts and cards (initial_state.agent_data); ids are unique,
            # so the union of all tasks is one bigger initial database
            tp = f"{TAU2}/data/tau2/domains/{dom}/tasks.json"
            for t in json.load(open(tp)) if os.path.exists(tp) else []:
                ad = (((t.get("initial_state") or {}).get("initialization_data") or {}).get("agent_data") or {})
                for k, v in ad.items():
                    if k != "task_config" and isinstance(v, dict):
                        self.tables.setdefault(k, {}).update(_unwrap(v))
        self.table_of = {}
        for tname, tab in self.tables.items():
            for rid in tab:
                self.table_of.setdefault(rid, tname)
        own, users = {}, set()
        for tname, tab in self.tables.items():
            if tname in ("users", "customers"):
                for rid, r in tab.items():
                    uid = r.get("user_id") or r.get("customer_id") or rid
                    users.add(uid)
                    own[rid] = uid
                    own[uid] = uid
                    ids = set()
                    _walk(r, ids)
                    for i in ids:
                        own.setdefault(i, uid)
        for tname, tab in self.tables.items():
            for rid, r in tab.items():
                if isinstance(r, dict):
                    u = r.get("user_id") or r.get("customer_id")
                    if u:
                        own[rid] = u
                        own.setdefault(u, u)
                        users.add(u)
        refs = {}
        for tname, tab in self.tables.items():
            for rid, r in tab.items():
                u = own.get(rid)
                if u is None or not isinstance(r, dict):
                    continue
                for k, v in r.items():
                    if k.endswith("_id") and isinstance(v, str) and v != rid:
                        refs.setdefault(v, set()).add(u)
        for i, us in refs.items():
            if i not in own and len(us) == 1:
                own[i] = next(iter(us))
        for tname, tab in self.tables.items():
            for rid, r in tab.items():
                if own.get(rid) is not None or not isinstance(r, dict):
                    continue
                us = {own[v] for k, v in r.items() if k.endswith("_id") and isinstance(v, str) and v != rid
                      and own.get(v)}
                if len(us) == 1:
                    own[rid] = next(iter(us))
        self.own, self.users = own, users

    def rec(self, table, rid):
        return (self.tables.get(table) or {}).get(rid) or {}

    def owner_of_args(self, args):
        found = []
        for k, v in (args or {}).items():
            vals = v if isinstance(v, list) else [v]
            for x in vals:
                if isinstance(x, dict):
                    found += self.owner_of_args(x)
                elif isinstance(x, str):
                    if k in ("user_id", "customer_id"):
                        found.append(x)
                    elif x in self.users:
                        found.append(x)
                    elif self.own.get(x):
                        found.append(self.own[x])
        return found


def owners_in_result(content, users=None):
    out = []
    if not content:
        return out
    for m in re.finditer(r'"(?:user_id|customer_id)"\s*:\s*"([^"]+)"', content):
        if users is None or m.group(1) in users:
            out.append(m.group(1))
    s = content.strip().strip('"')
    if users is not None and s in users:
        out.append(s)
    return out


# ---------------------------------------------------------------- documents of one call

class Doc:
    __slots__ = ("table", "id", "owner", "tags", "new")

    def __init__(self, table, did, owner, tags=(), new=False):
        self.table, self.id, self.owner, self.new = table, did, owner, new
        self.tags = set(tags)
        if owner:
            self.tags.add(f"owner:{owner}")

    @property
    def key(self):
        return (self.table, self.id)

    def matches(self, table, k):
        return table == self.table and (k == "*" or k == self.id or k in self.tags)


def _gift(pm):
    return isinstance(pm, str) and pm.startswith("gift_card")


class Mapper:
    """Maps one call (name, args, result) to (docs written) or (reads). One Mapper per run: it numbers new docs."""

    def __init__(self, db: DB, fallback_owner=None):
        self.db = db
        self.fallback = fallback_owner
        self.n_new = 0
        self.fallbacks = 0

    def _owner(self, rid=None, args=None):
        a = args or {}
        o = a.get("user_id") or a.get("customer_id")
        if o:
            return o
        if rid and self.db.own.get(rid):
            return self.db.own[rid]
        if args:
            ow = self.db.owner_of_args(args)
            if ow:
                return ow[0]
        self.fallbacks += 1
        return self.fallback

    def _new(self, table, owner, tags=()):
        self.n_new += 1
        return Doc(table, f"new{self.n_new}", owner, tags, new=True)

    def _upd(self, table, rid, args=None, tags=()):
        r = self.db.rec(table, rid)
        owner = r.get("user_id") or r.get("customer_id") or self._owner(rid, args)
        t = set(tags)
        for f in ("account_id", "credit_card_account_id"):
            if r.get(f):
                t.add(f"acct:{r[f]}")
        return Doc(table, rid, owner, t)

    # ------------------------------------------------------------ writes

    def writes(self, name, a):
        """Documents changed by a successful write call. Returns (docs, rule) or (None, None) if unknown."""
        d = self.db.dom
        if d == "retail":
            return self._w_retail(name, a)
        if d == "airline":
            return self._w_airline(name, a)
        if d == "telecom":
            return self._w_telecom(name, a)
        if d == "banking_knowledge":
            return self._w_bank(name, a)
        return None, None

    def _user_of_order(self, oid, a):
        r = self.db.rec("orders", oid)
        return r.get("user_id") or self._owner(oid, a)

    def _w_retail(self, n, a):
        oid = a.get("order_id")
        if n == "cancel_pending_order":
            # order.status = "cancelled"; refund to the original payment: a gift card balance lives in the user doc
            u = self._user_of_order(oid, a)
            docs = [self._upd("orders", oid, a)]
            hist = self.db.rec("orders", oid).get("payment_history") or []
            if any(_gift(p.get("payment_method_id")) for p in hist):
                docs.append(Doc("users", u, u))
            return docs, "order status; gift-card refund in user"
        if n in ("exchange_delivered_order_items", "return_delivered_order_items", "modify_pending_order_address"):
            # only order fields change (status, exchange/return fields, address)
            return [self._upd("orders", oid, a)], "order only"
        if n in ("modify_pending_order_items", "modify_pending_order_payment"):
            # order items/payment change; payment_method.balance -= / += on a gift card (user doc)
            u = self._user_of_order(oid, a)
            docs = [self._upd("orders", oid, a)]
            hist = self.db.rec("orders", oid).get("payment_history") or []
            if _gift(a.get("payment_method_id")) or (n == "modify_pending_order_payment"
                                                     and any(_gift(p.get("payment_method_id")) for p in hist)):
                docs.append(Doc("users", u, u))
            return docs, "order; gift card in user"
        if n == "modify_user_address":
            u = a.get("user_id")
            return [Doc("users", u, u)], "user address"
        return None, None

    def _w_airline(self, n, a):
        if n == "book_reservation":
            # new reservation; users[user_id].reservations.append; gift card amount; seats -= on each flight-date
            u = a.get("user_id")
            docs = [self._new("reservations", u), Doc("users", u, u)]
            fl = a.get("flights") or []
            if isinstance(fl, str):
                try:
                    fl = json.loads(fl)
                except Exception:
                    fl = []
            for f in fl:
                if isinstance(f, dict) and f.get("flight_number"):
                    docs.append(Doc("flights", f["flight_number"], None))
            return docs, "new reservation, user, flight seats (shared)"
        rid = a.get("reservation_id")
        if n in ("cancel_reservation", "update_reservation_passengers"):
            # reservation status/passengers; cancel does not release seats in tau2 ("not implemented")
            return [self._upd("reservations", rid, a)], "reservation only"
        if n in ("update_reservation_baggages", "update_reservation_flights"):
            # reservation; _payment_for_update: gift card amount -= in the user doc
            docs = [self._upd("reservations", rid, a)]
            if _gift(a.get("payment_id")):
                u = docs[0].owner
                docs.append(Doc("users", u, u))
            return docs, "reservation; gift card in user"
        if n == "send_certificate":
            u = a.get("user_id")
            return [Doc("users", u, u)], "certificate in user payment methods"
        return None, None

    def _w_telecom(self, n, a):
        c = a.get("customer_id")
        ln = a.get("line_id")
        if n in ("suspend_line", "resume_line", "enable_roaming", "disable_roaming"):
            return [Doc("lines", ln, c)], "line fields"
        if n == "send_payment_request":
            return [Doc("bills", a.get("bill_id"), c)], "bill status"
        if n == "refuel_data":
            # line.data_refueling_gb += ; a draft bill is created or updated and appended to customer.bill_ids
            return [Doc("lines", ln, c), Doc("bills", f"draft:{c}", c), Doc("customers", c, c)], \
                "line, draft bill, customer"
        return None, None

    def _w_bank(self, n, a):
        u = a.get("user_id")
        cc = a.get("credit_card_account_id")
        acc = a.get("account_id")
        card = a.get("card_id")

        def new(table, *tags):
            return self._new(table, u or self._owner(None, a), [t for t in tags if t])

        A = (lambda x: f"acct:{x}" if x else None)
        rules = {
            "change_user_email": lambda: [Doc("users", u, u)],
            "log_verification": lambda: [new("verification_history")],
            "update_transaction_rewards_3847": lambda: [self._upd("credit_card_transaction_history",
                                                                  a.get("transaction_id"), a)],
            "file_credit_card_transaction_dispute_4829": lambda: [new("transaction_disputes")],
            "file_debit_card_transaction_dispute_6281": lambda: [new("debit_card_disputes", A(acc))],
            "set_debit_card_recurring_block_7382": lambda: [self._upd("debit_cards", card, a)],
            "order_replacement_credit_card_7291": lambda: [new("credit_card_orders", A(cc))],
            "log_credit_card_closure_reason_4521": lambda: [new("credit_card_closure_reasons", A(cc))],
            "apply_statement_credit_8472": lambda: [new("credit_card_transaction_history", A(cc))],
            "apply_credit_card_account_flag_6147": lambda: [new("credit_card_account_flags", A(cc))],
            "close_credit_card_account_7834": lambda: [self._upd("credit_card_accounts", cc, a)],
            "pay_credit_card_from_checking_9182": lambda: [self._upd("accounts", a.get("checking_account_id"), a),
                                                           self._upd("credit_card_accounts", cc, a)],
            "submit_credit_limit_increase_request_7392": lambda: [new("credit_limit_increase_requests", A(cc))],
            "approve_credit_limit_increase_5847": lambda: [self._upd("credit_card_accounts", cc, a),
                                                           new("credit_limit_increase_requests", A(cc))],
            "deny_credit_limit_increase_5848": lambda: [new("credit_limit_increase_requests", A(cc))],
            "open_bank_account_4821": lambda: [new("accounts")],
            "close_bank_account_7392": lambda: [self._upd("accounts", acc, a)],
            "transfer_funds_between_bank_accounts_7291": lambda: [
                self._upd("accounts", a.get("source_account_id"), a),
                self._upd("accounts", a.get("destination_account_id"), a)],
            "apply_checking_account_credit_5829": lambda: [self._upd("accounts", acc, a)],
            "apply_savings_account_credit_6831": lambda: [self._upd("accounts", acc, a)],
            "submit_interest_discrepancy_report_7294": lambda: [new("interest_discrepancy_reports", A(acc))],
            "order_debit_card_5739": lambda: [new("debit_card_orders", A(acc)),
                                              new("bank_account_transaction_history", A(acc)),
                                              new("debit_cards", A(acc))],
            "request_temporary_debit_card_limit_increase_8374": lambda: [self._upd("debit_cards", card, a)],
            "submit_cash_back_dispute_0589": lambda: [new("cash_back_disputes")],
            "get_referral_link": lambda: [new("referrals")],
            "submit_referral": lambda: [new("referrals")],
            "deposit_check_3847": lambda: [self._upd("accounts", acc, a)],
            "apply_for_credit_card": lambda: [new("credit_card_applications")],
            "request_human_agent_transfer": lambda: [new("human_transfer_requests")],
            "submit_transaction": lambda: [new("credit_card_transaction_history")],
        }
        for k in ("activate_debit_card_8291", "activate_debit_card_8292", "activate_debit_card_8293",
                  "close_debit_card_4721", "freeze_debit_card_3892", "unfreeze_debit_card_3893",
                  "clear_debit_card_fraud_alert_4892", "reset_debit_card_pin_6284", "change_debit_card_pin_6285"):
            rules[k] = (lambda: [self._upd("debit_cards", card, a)])
        f = rules.get(n)
        if f is None:
            return None, None
        return f(), "banking rule"

    # ------------------------------------------------------------ reads

    def reads(self, name, a, content=""):
        """(table, key) list read by a successful read call, and the customers it reveals."""
        d = self.db.dom
        u = a.get("user_id") or a.get("customer_id")
        res_owners = owners_in_result(content)
        if d == "retail":
            if name == "get_user_details":
                return [("users", u)]
            if name == "get_order_details":
                return [("orders", a.get("order_id"))]
            if name in ("find_user_id_by_name_zip", "find_user_id_by_email"):
                s = (content or "").strip().strip('"')
                return [("users", s)] if s else []
            if name == "get_product_details":
                return [("products", a.get("product_id"))]
            if name == "get_item_details":
                return [("products", "item:" + str(a.get("item_id")))]
            if name == "list_all_product_types":
                return [("products", "*")]
        if d == "airline":
            if name == "get_user_details":
                return [("users", u)]
            if name == "get_reservation_details":
                return [("reservations", a.get("reservation_id"))]
            if name in ("search_direct_flight", "search_onestop_flight"):
                o, de = a.get("origin"), a.get("destination")
                out = []
                for fn, f in self.db.tables.get("flights", {}).items():
                    if name == "search_direct_flight" and f.get("origin") == o and f.get("destination") == de:
                        out.append(("flights", fn))
                    if name == "search_onestop_flight" and (f.get("origin") == o or f.get("destination") == de):
                        out.append(("flights", fn))
                return out
            if name == "get_flight_status":
                return [("flights", a.get("flight_number"))]
        if d == "telecom":
            if name in ("get_customer_by_phone", "get_customer_by_name"):
                return [("customers", x) for x in res_owners]
            if name == "get_customer_by_id":
                return [("customers", u)]
            if name == "get_details_by_id":
                i = str(a.get("id") or "")
                t = {"C": "customers", "L": "lines", "D": "devices", "B": "bills", "P": "plans"}.get(i[:1], "?")
                return [(t, i)]
            if name == "get_bills_for_customer":
                return [("bills", f"owner:{u}")]
            if name == "get_data_usage":
                return [("lines", a.get("line_id"))]
        if d == "banking_knowledge":
            cc = a.get("credit_card_account_id")
            acc = a.get("account_id")
            m = {
                "get_user_information_by_id": [("users", u)],
                "get_referrals_by_user": [("referrals", f"owner:{u}")],
                "get_credit_card_transactions_by_user": [("credit_card_transaction_history", f"owner:{u}")],
                "get_credit_card_accounts_by_user": [("credit_card_accounts", f"owner:{u}")],
                "get_debit_dispute_status_7483": [("debit_card_disputes", f"owner:{u}")],
                "get_atm_deposit_images_8473": [("bank_account_transaction_history", a.get("transaction_id"))],
                "get_user_dispute_history_7291": [("transaction_disputes", f"owner:{u}")],
                "get_pending_replacement_orders_5765": [("credit_card_orders", f"acct:{cc}")],
                "get_closure_reason_history_8293": [("credit_card_closure_reasons", f"acct:{cc}")],
                "get_credit_limit_increase_history_4829": [("credit_limit_increase_requests", f"acct:{cc}")],
                "get_payment_history_6183": [("payment_history", f"acct:{cc}")],
                "get_all_user_accounts_by_user_id_3847": [("accounts", f"owner:{u}")],
                "get_bank_account_transactions_9173": [("bank_account_transaction_history", f"acct:{acc}")],
                "get_debit_cards_by_account_id_7823": [("debit_cards", f"acct:{acc}")],
                "get_card_last_4_digits": [("credit_card_accounts", cc)],
            }
            if name in ("get_user_information_by_name", "get_user_information_by_email"):
                return [("users", x) for x in res_owners]
            if name in m:
                return m[name]
        return []

    def read_owners(self, reads):
        out = set()
        for t, k in reads:
            if k is None:
                continue
            if k.startswith("owner:"):
                out.add(k[6:])
            elif k.startswith("acct:"):
                o = self.db.own.get(k[5:])
                if o:
                    out.add(o)
            elif t in ("users", "customers"):
                out.add(k)
            elif self.db.own.get(k):
                out.add(self.db.own[k])
        return out
