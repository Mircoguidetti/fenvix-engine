"""Generatore dei profili del catalogo del ristorante (86 app, sessione Fable 2026-11-01).

Ogni app è descritta da: numero di database, forma del traffico, ritmo quando è attiva
(operazioni al secondo per database), e un elenco pesato di modelli di lettura e scrittura.
I numeri sono STIMATI dal rapporto del catalogo (sezioni 1 e 3); il generatore li trasforma
in un flusso di eventi per analizza.py.

Modelli:
  P(coll, w, card, z)                  lettura puntuale per _id
  F(coll, w, field, card, z, uniq)     find con uguaglianza su field (più sort/limit)
  C(coll, w, field, card, z, uniq)     conteggio
  A(coll, w, field, card, z, uniq)     aggregazione (field=None: tutta la collezione)
  I(coll, w)                           inserimento
  U(coll, w, card, z, chg)             update per _id (chg: campi di partizione cambiati)
  M(coll, w, field, card, z)           updateMany/deleteMany per valore di field
uniq = probabilità che la query abbia un parametro unico (cursore, testo cercato, "adesso")
che la rende irripetibile.
I campi di partizione di ogni collezione sono in schema {coll: {field: (card, z)}}.
"""
import bisect
import hashlib
import math
import random

# ---------------------------------------------------------------- distribuzioni
_cdf_cache = {}


def zipf_cdf(n, s):
    key = (n, s)
    c = _cdf_cache.get(key)
    if c is None:
        acc, c = 0.0, []
        for i in range(1, n + 1):
            acc += 1.0 / (i ** s)
            c.append(acc)
        c = [x / acc for x in c]
        _cdf_cache[key] = c
    return c


def zipf(rng, n, s):
    if s <= 0:
        return rng.randrange(n)
    return min(bisect.bisect_left(zipf_cdf(n, s), rng.random()), n - 1)


def zipf_from_u(u, n, s):
    if s <= 0:
        return int(u * n) % n
    return min(bisect.bisect_left(zipf_cdf(n, s), u), n - 1)


def h01(*parts):
    d = hashlib.blake2b(repr(parts).encode(), digest_size=8).digest()
    return int.from_bytes(d, "little") / 2.0**64


# ---------------------------------------------------------------- modelli
def P(coll, w, card, z=1.0):
    return ("point", coll, w, None, card, z, 0.0)


def F(coll, w, field, card, z=1.0, uniq=0.0):
    return ("filter", coll, w, field, card, z, uniq)


def C(coll, w, field, card, z=1.0, uniq=0.0):
    return ("count", coll, w, field, card, z, uniq)


def A(coll, w, field, card, z=1.0, uniq=0.0):
    return ("agg", coll, w, field, card, z, uniq)


def I(coll, w):
    return ("ins", coll, w, None, 0, 0, None)


def U(coll, w, card, z=1.0, chg=()):
    return ("upd", coll, w, None, card, z, tuple(chg))


def M(coll, w, field, card, z=1.0):
    return ("many", coll, w, field, card, z, None)


class App:
    def __init__(self, num, name, ndb, shape, rate, schema, ops, active_frac=1.0, peaks=()):
        self.num, self.name, self.ndb, self.shape, self.rate = num, name, ndb, shape, rate
        self.schema, self.ops, self.active_frac, self.peaks = schema, ops, active_frac, peaks


S = dict  # abbreviazione per gli schemi

# Forme del traffico (finestra simulata: 08:00-12:00 di un giorno feriale, 14.400 s)
#  always  : mai dorme, ritmo costante
#  day     : costante nelle ore d'ufficio
#  burst   : 3 scatti all'ora da 2-10 minuti, fermo tra uno scatto e l'altro
#  sleep   : quasi sempre fermo, poche operazioni sparse (rate = operazioni all'ora)
#  peak    : fermo tranne le finestre in peaks [(inizio_s, durata_s, molt)]
APPS = [
    # ---- A. Commercio
    App(1, "negozio", 1, "burst", 3.0,
        S(products={"category": (40, 1.0)}, orders={"userId": (5000, 0.9)}, users={}),
        [P("products", 30, 2000, 1.0), F("products", 20, "category", 40, 1.0, 0.3),
         F("products", 5, "category", 40, 1.0, 0.9),  # $text
         F("orders", 6, "userId", 5000, 0.9, 0.1), A("orders", 1, None, 1, 0, 0.5),
         P("users", 10, 5000, 0.9),
         U("products", 4, 2000, 1.0), I("orders", 3), U("users", 1, 5000, 0.9)]),
    App(2, "marketplace", 1, "day", 4.0,
        S(listings={"sellerId": (800, 1.0), "category": (60, 1.0), "status": (3, 0.5)},
          offers={"listingId": (20000, 1.0)}, orders={"sellerId": (800, 1.0)}, sellers={}),
        [F("listings", 25, "category", 60, 1.0, 0.4), C("listings", 15, "category", 60, 1.0, 0.4),
         F("listings", 10, "sellerId", 800, 1.0), F("offers", 10, "listingId", 20000, 1.0),
         A("orders", 3, "sellerId", 800, 1.0), P("listings", 20, 20000, 1.0), P("sellers", 5, 800, 1.0),
         I("offers", 6), U("listings", 4, 20000, 1.0, ["status"]), I("orders", 2),
         M("offers", 0.2, "listingId", 20000, 1.0)]),
    App(3, "magazzino", 1, "day", 1.5,
        S(items={"location": (20, 0.8)}, stock_movements={"itemId": (5000, 1.0)}),
        [P("items", 40, 5000, 1.0), A("items", 3, None, 1, 0, 0.0), F("stock_movements", 10, "itemId", 5000, 1.0),
         U("items", 30, 5000, 1.0), I("stock_movements", 30)]),
    App(4, "catalogo-headless", 1, "day", 6.0,
        S(products={"brand": (200, 1.0)}, brands={}),
        [F("products", 30, "brand", 200, 1.0, 0.6), A("products", 20, "brand", 200, 1.0, 0.5),
         P("products", 35, 10000, 1.0), F("brands", 5, None, 1, 0),
         U("products", 2, 10000, 1.0), I("products", 0.5)]),
    App(5, "carrello-coupon", 1, "burst", 2.0,
        S(carts={}, coupons={}),
        [P("carts", 40, 3000, 0.7), P("coupons", 15, 200, 1.2),
         U("carts", 35, 3000, 0.7), U("coupons", 5, 200, 1.2), I("carts", 5)]),
    App(6, "abbonamenti", 2, "peak", 0.3,
        S(customers={}, subscriptions={"customerId": (3000, 0.8), "status": (4, 0.5)}, invoices={"customerId": (3000, 0.8)}),
        [P("customers", 30, 3000, 0.8), F("subscriptions", 30, "customerId", 3000, 0.8),
         F("invoices", 20, "customerId", 3000, 0.8, 0.2), A("invoices", 2, None, 1, 0, 0.5),
         U("subscriptions", 10, 3000, 0.8, ["status"]), I("invoices", 8)], peaks=()),  # picco alle 2 di notte, fuori finestra
    App(7, "recensioni", 1, "burst", 1.5,
        S(reviews={"productId": (2000, 1.0)}, product_stats={}),
        [F("reviews", 40, "productId", 2000, 1.0, 0.2), P("product_stats", 30, 2000, 1.0),
         A("reviews", 5, "productId", 2000, 1.0), I("reviews", 10), U("product_stats", 10, 2000, 1.0)]),
    App(8, "dropshipping", 1, "peak", 0.05,
        S(products_mirror={}, price_history={"sku": (10000, 0.5)}),
        [P("products_mirror", 10, 10000, 0.5), U("products_mirror", 80, 10000, 0.0), I("price_history", 10)],
        peaks=[(h * 3600 + 600, 120, 1700.0) for h in range(4)]),
    # ---- B. Contenuti
    App(9, "blog", 1, "sleep", 1.0,
        S(posts={"tags": (30, 1.0)}, comments={"postId": (200, 1.0)}, users={}),
        [P("posts", 40, 200, 1.0), F("posts", 30, None, 1, 0, 0.3), F("comments", 20, "postId", 200, 1.0),
         P("users", 10, 50, 1.0), I("comments", 2)]),
    App(10, "cms-headless", 1, "burst", 2.0,
        S(pages={}, articles={"status": (3, 0.8)}, _versions_articles={"parent": (500, 1.0)}, media={}),
        [P("articles", 25, 500, 1.0), F("articles", 25, "status", 3, 0.8, 0.3), P("pages", 15, 100, 1.0),
         P("media", 15, 2000, 1.0), F("_versions_articles", 3, "parent", 500, 1.0),
         U("articles", 8, 500, 1.0, ["status"]), I("_versions_articles", 8), M("_versions_articles", 0.2, "parent", 500, 1.0)]),
    App(11, "wiki", 1, "day", 1.0,
        S(pages={"parentId": (300, 1.0)}, revisions={"pageId": (3000, 1.0)}),
        [P("pages", 40, 3000, 1.0), F("pages", 15, "parentId", 300, 1.0), F("pages", 10, None, 1, 0, 0.9),
         A("pages", 5, "parentId", 300, 1.0), F("revisions", 5, "pageId", 3000, 1.0),
         U("pages", 5, 3000, 1.0), I("revisions", 5)]),
    App(12, "newsletter", 1, "peak", 0.02,
        S(subscribers={}, events={"campaignId": (50, 1.5)}, campaigns={}),
        [P("subscribers", 5, 50000, 0.3), A("events", 3, "campaignId", 50, 1.5, 0.3), P("campaigns", 2, 50, 1.5),
         I("events", 90)], peaks=[(5400, 600, 4000.0)]),
    App(13, "sito-vetrina", 1, "sleep", 0.15,
        S(pages={}), [P("pages", 100, 5, 1.0)]),
    App(14, "commenti", 1, "burst", 2.0,
        S(comments={"threadId": (1000, 1.1)}, votes={}),
        [F("comments", 50, "threadId", 1000, 1.1, 0.2), A("comments", 5, "threadId", 1000, 1.1),
         I("comments", 10), U("comments", 15, 20000, 1.0), I("votes", 15), P("votes", 5, 20000, 1.0)]),
    App(15, "archivio-gridfs", 1, "day", 0.3,
        S(**{"fs.files": {"owner": (100, 1.0)}, "fs.chunks": {"files_id": (5000, 1.0)}, "documents_meta": {}}),
        [F("fs.chunks", 50, "files_id", 5000, 1.0), F("fs.files", 15, "owner", 100, 1.0, 0.5),
         P("documents_meta", 10, 5000, 1.0), I("fs.chunks", 20), I("fs.files", 2)]),
    # ---- C. Chat, social
    App(16, "chat", 2, "day", 8.0,
        S(messages={"conversationId": (3000, 1.0)}, conversations={"members": (5000, 1.0)}, users={}),
        [F("messages", 30, "conversationId", 3000, 1.0, 0.3), F("conversations", 10, "members", 5000, 1.0),
         P("users", 10, 5000, 1.0), I("messages", 35), M("messages", 8, "conversationId", 3000, 1.0),
         U("messages", 5, 100000, 1.0)]),
    App(17, "assistenza", 1, "day", 2.0,
        S(rooms={"status": (3, 0.5)}, messages={"roomId": (500, 1.0)}, presence={}, agents={}),
        [F("messages", 30, "roomId", 500, 1.0, 0.2), C("rooms", 15, "status", 3, 0.5),
         F("messages", 5, None, 1, 0, 0.95), P("agents", 10, 30, 1.0),
         I("messages", 20), U("rooms", 10, 500, 1.0, ["status"]), U("presence", 10, 30, 0.5)]),
    App(18, "feed-social", 1, "burst", 10.0,
        S(posts={"authorId": (5000, 1.0)}, feeds={"userId": (5000, 1.0)}, likes={"postId": (50000, 1.0)}, follows={"followerId": (5000, 1.0)}),
        [F("feeds", 40, "userId", 5000, 1.0, 0.4), P("posts", 15, 50000, 1.1), A("posts", 2, None, 1, 0, 0.5),
         F("follows", 5, "followerId", 5000, 1.0),
         I("feeds", 25), I("posts", 1), I("likes", 8), U("posts", 6, 50000, 1.1)]),
    App(19, "forum", 1, "day", 1.0,
        S(questions={"tag": (100, 1.0)}, answers={"questionId": (5000, 1.0)}, votes={}),
        [P("questions", 30, 5000, 1.0), F("answers", 25, "questionId", 5000, 1.0), F("questions", 15, "tag", 100, 1.0, 0.3),
         F("questions", 5, None, 1, 0, 0.9), A("questions", 2, None, 1, 0, 0.3),
         I("answers", 5), I("votes", 10), U("questions", 6, 5000, 1.0)]),
    App(20, "notifiche", 1, "day", 5.0,
        S(notifications={"userId": (10000, 1.0)}, devices={}),
        [C("notifications", 35, "userId", 10000, 1.0), F("notifications", 20, "userId", 10000, 1.0, 0.3),
         I("notifications", 30), M("notifications", 10, "userId", 10000, 1.0), P("devices", 5, 10000, 1.0)]),
    App(21, "sondaggi", 1, "peak", 0.01,
        S(polls={}, votes={"pollId": (3, 1.0)}),
        [P("polls", 20, 3, 1.0), U("polls", 40, 3, 1.0), I("votes", 40)], peaks=[(7200, 60, 8000.0)]),
    App(22, "presenza", 1, "always", 500.0,
        S(presence={}),
        [U("presence", 96, 10000, 0.0), F("presence", 4, None, 1, 0, 1.0)]),  # lista amici con $in: irripetibile
    App(23, "lavagna", 1, "burst", 20.0,
        S(ops={"docId": (50, 1.0)}, snapshots={}, docs={}),
        [P("snapshots", 3, 50, 1.0), F("ops", 5, "docId", 50, 1.0, 0.9), P("docs", 2, 50, 1.0), I("ops", 90)]),
    # ---- D. Prenotazioni, geo
    App(24, "prenota-tavoli", 1, "day", 3.0,
        S(restaurants={"area": (50, 1.0)}, tables={"restaurantId": (300, 1.0)}, reservations={"restaurantId": (300, 1.0)}),
        [F("restaurants", 30, "area", 50, 1.0, 0.6), C("reservations", 20, "restaurantId", 300, 1.0, 0.3),
         P("restaurants", 20, 300, 1.0), F("tables", 10, "restaurantId", 300, 1.0),
         I("reservations", 8), U("tables", 8, 3000, 1.0)]),
    App(25, "consegne", 1, "day", 220.0,
        S(couriers={"zone": (30, 0.8)}, orders={"courierId": (400, 1.0)}),
        [F("couriers", 5, "zone", 30, 0.8, 0.9), P("orders", 3, 5000, 1.0), F("orders", 2, "courierId", 400, 1.0),
         U("couriers", 88, 400, 0.0, ["zone"]), I("orders", 1), U("orders", 1, 5000, 1.0)]),
    App(26, "agenda", 1, "burst", 1.5,
        S(providers={}, slots={"providerId": (100, 1.0)}, bookings={"providerId": (100, 1.0)}, holds={}),
        [F("slots", 40, "providerId", 100, 1.0, 0.3), P("providers", 20, 100, 1.0), F("bookings", 10, "providerId", 100, 1.0, 0.3),
         U("slots", 10, 5000, 1.0), I("bookings", 5), I("holds", 5)]),
    App(27, "alloggi", 1, "day", 4.0,
        S(properties={"area": (100, 1.0)}, availability={"propertyId": (3000, 1.0)}, bookings={}, reviews={"propertyId": (3000, 1.0)}),
        [F("properties", 30, "area", 100, 1.0, 0.7), F("availability", 25, "propertyId", 3000, 1.0, 0.4),
         P("properties", 20, 3000, 1.0), F("reviews", 10, "propertyId", 3000, 1.0), A("availability", 3, "propertyId", 3000, 1.0, 0.5),
         U("availability", 8, 100000, 1.0), I("bookings", 2)]),
    App(28, "bici", 1, "day", 30.0,
        S(vehicles={"zone": (40, 0.8), "status": (3, 0.5)}, rides={}, positions={"vehicleId": (1000, 0.0)}),
        [F("vehicles", 15, "zone", 40, 0.8, 0.8), A("positions", 2, None, 1, 0, 0.5),
         I("positions", 75), U("vehicles", 6, 1000, 0.5, ["status"]), I("rides", 2)]),
    App(29, "biglietteria", 1, "peak", 0.02,
        S(events={}, tickets={"eventId": (5, 1.5)}, holds={}, orders={}),
        [P("events", 25, 5, 1.5), C("tickets", 20, "eventId", 5, 1.5), I("tickets", 20), I("holds", 15),
         U("events", 10, 5, 1.5), I("orders", 10)], peaks=[(9000, 120, 4000.0)]),
    App(30, "trasporti", 1, "always", 320.0,
        S(stops={"zone": (100, 1.0)}, trips={}, vehicle_positions={"vehicleId": (300, 0.0)}),
        [F("stops", 3, "zone", 100, 1.0, 0.5), A("vehicle_positions", 3, None, 1, 0, 0.3), P("trips", 2, 2000, 1.0),
         I("vehicle_positions", 92)]),
    # ---- E. Gestionali
    App(31, "crm", 1, "day", 1.5,
        S(contacts={"companyId": (300, 1.0)}, companies={}, deals={"ownerId": (10, 0.8)}, activities={"contactId": (3000, 1.0)}),
        [P("contacts", 30, 3000, 1.0), F("contacts", 10, None, 1, 0, 0.9), F("deals", 15, "ownerId", 10, 0.8),
         A("deals", 8, None, 1, 0, 0.0), F("activities", 10, "contactId", 3000, 1.0), P("companies", 10, 300, 1.0),
         U("contacts", 5, 3000, 1.0), U("deals", 4, 1000, 1.0), I("activities", 6)]),
    App(32, "crm-multitenant", 40, "day", 1.0,
        S(contacts={"companyId": (100, 1.0)}, companies={}, deals={"ownerId": (5, 0.8)}, activities={"contactId": (1000, 1.0)}),
        [P("contacts", 30, 1000, 1.0), F("contacts", 10, None, 1, 0, 0.9), F("deals", 15, "ownerId", 5, 0.8),
         A("deals", 8, None, 1, 0, 0.0), F("activities", 10, "contactId", 1000, 1.0), P("companies", 10, 100, 1.0),
         U("contacts", 5, 1000, 1.0), U("deals", 4, 300, 1.0), I("activities", 6)], active_frac=0.2),
    App(33, "fatturazione", 1, "day", 1.0,
        S(invoices={"customerId": (500, 1.0)}, customers={}, ledger_entries={"account": (50, 1.0)}, counters={}),
        [F("invoices", 30, "customerId", 500, 1.0, 0.2), P("customers", 20, 500, 1.0), A("ledger_entries", 8, "account", 50, 1.0, 0.3),
         P("invoices", 15, 10000, 1.0), U("counters", 4, 3, 1.0), I("invoices", 4), I("ledger_entries", 8)]),
    App(34, "presenze", 1, "peak", 0.3,
        S(employees={}, timesheets={"employeeId": (200, 0.0)}, leave_requests={"employeeId": (200, 0.5)}),
        [P("employees", 20, 200, 0.5), F("timesheets", 20, "employeeId", 200, 0.3), A("timesheets", 5, "employeeId", 200, 0.5),
         U("timesheets", 40, 200, 0.0), I("leave_requests", 2)], peaks=[(0, 3600, 20.0)]),
    App(35, "helpdesk", 1, "day", 2.0,
        S(tickets={"assigneeId": (20, 0.8), "status": (4, 0.5)}, comments={"ticketId": (5000, 1.0)}, agents={}),
        [F("tickets", 30, "assigneeId", 20, 0.8, 0.1), C("tickets", 15, "status", 4, 0.5), P("tickets", 20, 5000, 1.0),
         F("comments", 10, "ticketId", 5000, 1.0), F("tickets", 3, None, 1, 0, 0.95),
         U("tickets", 10, 5000, 1.0, ["status", "assigneeId"]), I("comments", 8), I("tickets", 2)]),
    App(36, "kanban", 1, "day", 2.0,
        S(boards={"workspaceId": (20, 1.0)}, lists={"boardId": (100, 1.0)}, cards={"listId": (600, 1.0)}, comments={"cardId": (5000, 1.0)}),
        [F("cards", 30, "listId", 600, 1.0), F("lists", 15, "boardId", 100, 1.0), F("boards", 10, "workspaceId", 20, 1.0),
         P("cards", 10, 5000, 1.0), F("comments", 10, "cardId", 5000, 1.0, 0.2),
         U("cards", 10, 5000, 1.0, ["listId"]), M("cards", 3, "listId", 600, 1.0), I("comments", 4), I("cards", 2)]),
    App(37, "scuola", 1, "burst", 1.0,
        S(students={}, courses={}, enrollments={"studentId": (2000, 1.0), "courseId": (100, 1.0)}, grades={"courseId": (100, 1.0)}),
        [F("enrollments", 30, "studentId", 2000, 1.0), P("courses", 25, 100, 1.0), P("students", 15, 2000, 1.0),
         A("grades", 10, "courseId", 100, 1.0), I("grades", 8), I("enrollments", 4)]),
    App(38, "moduli", 1, "burst", 2.0,
        S(forms={}, submissions={"formId": (50, 1.2)}, reports={}),
        [P("forms", 40, 50, 1.2), A("submissions", 5, "formId", 50, 1.2), P("reports", 5, 50, 1.2), I("submissions", 50)]),
    App(39, "pos", 1, "peak", 0.5,
        S(menu={}, orders={"status": (3, 0.5), "shiftId": (5, 1.0)}, shifts={}, payments={}),
        [P("menu", 35, 80, 1.0), F("orders", 20, "status", 3, 0.5), A("orders", 3, "shiftId", 5, 1.0),
         I("orders", 15), U("orders", 15, 5000, 1.5, ["status"]), U("menu", 8, 80, 1.0), I("payments", 4)],
        peaks=[(12600, 1800, 20.0)]),
    # ---- F. AI
    App(40, "memoria-langgraph", 20, "burst", 1.0,
        S(checkpoints={"thread_id": (300, 1.0)}, checkpoint_writes={"thread_id": (300, 1.0)}),
        [F("checkpoints", 45, "thread_id", 300, 1.0), I("checkpoints", 30), I("checkpoint_writes", 25)], active_frac=0.3),
    App(41, "memoria-mem0", 1, "burst", 2.0,
        S(memories={"user_id": (2000, 1.0)}),
        [F("memories", 60, "user_id", 2000, 1.0), U("memories", 25, 50000, 1.0), I("memories", 15)]),
    App(42, "rag", 2, "burst", 2.0,
        S(chunks={"tenant": (20, 1.0), "source": (500, 1.0)}, documents={}, ingestion_jobs={}),
        [F("chunks", 60, "source", 500, 1.0, 0.3), F("chunks", 20, "tenant", 20, 1.0, 0.3), P("documents", 10, 500, 1.0),
         I("chunks", 10)]),
    App(43, "chatbot", 1, "burst", 3.0,
        S(chats={"userId": (2000, 1.0)}, messages={"chatId": (10000, 1.0)}, votes={}),
        [F("chats", 25, "userId", 2000, 1.0), F("messages", 30, "chatId", 10000, 1.0), P("chats", 5, 10000, 1.0),
         I("messages", 15), U("messages", 20, 10000, 1.0), U("votes", 5, 10000, 1.0)]),
    App(44, "tracce-modelli", 1, "day", 10.0,
        S(traces={}, spans={"traceId": (100000, 0.5)}, costs={"model": (8, 1.0)}),
        [F("spans", 5, "traceId", 100000, 0.5), A("costs", 2, "model", 8, 1.0, 0.5), I("spans", 70), I("traces", 13), I("costs", 10)]),
    App(45, "agente-schema", 10, "burst", 2.0,
        S(**{f"c{i}": {"f": (50, 1.0)} for i in range(6)}),
        [F(f"c{i}", 8, "f", 50, 1.0, 0.5) for i in range(6)] + [A("c0", 5, None, 1, 0, 0.5)] +
        [I(f"c{i}", 6) for i in range(6)] + [U("c1", 4, 2000, 1.0)], active_frac=0.3),
    App(46, "coda-agenti", 1, "always", 3.0,
        S(tasks={"status": (4, 0.5)}),
        [C("tasks", 10, "status", 4, 0.5), I("tasks", 30), U("tasks", 60, 100000, 0.0, ["status"])]),
    App(47, "cache-modelli", 1, "always", 40.0,
        S(cache={}),
        [P("cache", 75, 200000, 0.9), U("cache", 15, 200000, 0.9), I("cache", 10)]),
    App(48, "base-conoscenza", 1, "burst", 2.0,
        S(articles={"category": (40, 1.0)}, chunks={"articleId": (2000, 1.0)}, feedback={}, stats={}),
        [P("articles", 30, 2000, 1.0), F("articles", 15, None, 1, 0, 0.9), F("chunks", 25, "articleId", 2000, 1.0),
         F("articles", 10, "category", 40, 1.0), U("articles", 5, 2000, 1.0), I("feedback", 5)]),
    App(49, "assistente-mcp", 1, "sleep", 60.0,
        S(notes={}, reminders={"dueAt": (100, 0.0)}, preferences={}),
        [P("preferences", 30, 1, 0), F("reminders", 20, "dueAt", 100, 0.0, 0.8), F("notes", 10, None, 1, 0, 0.9),
         U("notes", 25, 500, 1.0), U("reminders", 10, 200, 1.0)]),
    App(50, "eval", 1, "peak", 0.02,
        S(datasets={}, runs={}, results={"runId": (50, 1.5)}),
        [A("results", 10, "runId", 50, 1.5), P("runs", 10, 50, 1.5), P("datasets", 5, 20, 1.0), I("results", 75)],
        peaks=[(11000, 300, 300.0)]),
    # ---- G. IoT, log
    App(51, "sensori", 1, "always", 205.0,
        S(readings={"deviceId": (200, 0.0)}, devices={}, alerts={}),
        [A("readings", 1, "deviceId", 200, 1.0, 0.5), P("devices", 1, 200, 1.0), I("readings", 98)]),
    App(52, "telemetria-secchielli", 1, "always", 2000.0,
        S(buckets={"machineId": (500, 0.0)}),
        [A("buckets", 0.5, "machineId", 500, 1.0, 0.5), U("buckets", 99.5, 500, 0.0)]),
    App(53, "log", 1, "always", 1000.0,
        S(logs={"service": (30, 1.0)}, logs_capped={}, services={}),
        [F("logs", 0.2, "service", 30, 1.0, 0.7), A("logs", 0.05, None, 1, 0, 0.5), I("logs", 99.75)]),
    App(54, "monitoraggio", 1, "always", 8.5,
        S(monitors={}, heartbeats={"monitorId": (500, 0.0)}, status_pages={}),
        [F("heartbeats", 3, "monitorId", 500, 1.0), P("status_pages", 2, 10, 1.0), A("heartbeats", 1, "monitorId", 500, 1.0),
         P("monitors", 2, 500, 0.0), I("heartbeats", 92)]),
    App(55, "statistiche-web", 1, "day", 30.0,
        S(events={"siteId": (20, 1.0)}, sessions={}, pageviews={"siteId": (20, 1.0)}),
        [A("pageviews", 1, "siteId", 20, 1.0, 0.5), A("events", 1, "siteId", 20, 1.0, 0.5), I("events", 60), I("pageviews", 30), U("sessions", 8, 5000, 0.5)]),
    App(56, "fitness", 1, "burst", 3.0,
        S(users={}, workouts={"userId": (3000, 1.0)}, samples={"userId": (3000, 1.0)}),
        [F("workouts", 10, "userId", 3000, 1.0), A("samples", 5, "userId", 3000, 1.0), P("users", 5, 3000, 1.0), I("samples", 75), I("workouts", 5)]),
    App(57, "contatori-energia", 1, "always", 5.0,
        S(meter_readings={"meterId": (2000, 0.0)}, tariffs={}, bills={"meterId": (2000, 1.0)}, daily={"meterId": (2000, 1.0)}),
        [F("daily", 5, "meterId", 2000, 1.0), P("tariffs", 3, 10, 1.0), F("bills", 2, "meterId", 2000, 1.0), I("meter_readings", 90)]),
    # ---- H. Giochi
    App(58, "classifica", 1, "day", 10.0,
        S(scores={"game": (5, 1.0)}),
        [F("scores", 30, "game", 5, 1.0), C("scores", 25, "game", 5, 1.0, 0.9), P("scores", 10, 20000, 1.0), U("scores", 35, 20000, 1.0)]),
    App(59, "gioco-salvataggi", 1, "day", 5.0,
        S(players={}, saves={}, inventory={"playerId": (5000, 1.0)}, purchases={}),
        [P("players", 25, 5000, 1.0), P("saves", 15, 5000, 1.0), F("inventory", 15, "playerId", 5000, 1.0),
         U("players", 15, 5000, 1.0), U("saves", 15, 5000, 1.0), U("inventory", 10, 5000, 1.0), I("purchases", 5)]),
    App(60, "partite-turni", 1, "burst", 2.0,
        S(matches={"playerId": (3000, 1.0)}, moves={"matchId": (2000, 1.0)}),
        [P("matches", 30, 2000, 1.0), F("matches", 15, "playerId", 3000, 1.0), F("moves", 10, "matchId", 2000, 1.0),
         U("matches", 25, 2000, 1.0), I("moves", 20)]),
    App(61, "economia", 1, "day", 3.0,
        S(wallets={}, transactions={"walletId": (5000, 1.0)}, items={}),
        [P("wallets", 35, 5000, 1.0), F("transactions", 15, "walletId", 5000, 1.0, 0.3), P("items", 15, 300, 1.0),
         U("wallets", 20, 5000, 1.0), I("transactions", 15)]),
    App(62, "matchmaking", 1, "day", 5.0,
        S(queue={"region": (6, 0.8)}),
        [C("queue", 30, "region", 6, 0.8), I("queue", 35), U("queue", 35, 50000, 0.0, ["region"])]),
    # ---- I. Analisi
    App(63, "bi", 1, "day", 2.5,
        S(fact_sales={"storeId": (50, 1.0)}, dim_products={}, dim_stores={}, reports={}),
        [A("fact_sales", 80, None, 1, 0, 0.3), A("fact_sales", 10, "storeId", 50, 1.0, 0.3), P("dim_products", 3, 1000, 1.0),
         P("reports", 5, 30, 1.0), I("reports", 2)]),
    App(64, "etl-notturno", 1, "peak", 0.0, S(clean={}), [I("clean", 100)], peaks=()),
    App(65, "report-pdf", 1, "peak", 0.0, S(orders_archive={}), [A("orders_archive", 100, None, 1, 0, 0.0)], peaks=()),
    App(66, "interruttori", 1, "always", 50.0,
        S(flags={}, exposures={"variant": (4, 0.5)}, results={}),
        [P("flags", 60, 20, 1.5), A("exposures", 0.2, "variant", 4, 0.5, 0.3), I("exposures", 39), U("flags", 0.02, 20, 1.5)]),
    App(67, "questionari", 1, "burst", 1.0,
        S(responses={"surveyId": (20, 1.0)}),
        [F("responses", 30, None, 1, 0, 0.8), A("responses", 30, "surveyId", 20, 1.0, 0.3), I("responses", 40)]),
    App(68, "arricchimento", 1, "sleep", 10.0,
        S(records={"status": (3, 0.5)}),
        [F("records", 20, "status", 3, 0.5, 0.9), M("records", 80, "status", 3, 0.5)]),
    # ---- J. Code, infrastruttura
    App(69, "coda-agenda", 1, "always", 0.25,
        S(agendaJobs={"name": (10, 1.0)}),
        [F("agendaJobs", 5, "name", 10, 1.0), U("agendaJobs", 95, 50, 1.0, ["name"])]),  # findOneAndUpdate a vuoto: è una scrittura
    App(70, "limitatore", 1, "always", 1500.0,
        S(counters={}), [U("counters", 100, 40, 1.0)]),
    App(71, "sessioni-web", 1, "always", 20.0,
        S(sessions={}), [P("sessions", 50, 3000, 0.8), U("sessions", 48, 3000, 0.8), I("sessions", 2)]),
    App(72, "configurazione", 1, "sleep", 2.0,
        S(configs={}), [P("configs", 95, 30, 1.0), U("configs", 5, 30, 1.0)]),
    App(73, "file-grandi", 1, "burst", 3.0,
        S(**{"fs.files": {"owner": (100, 1.0)}, "fs.chunks": {"files_id": (300, 1.0)}}),
        [F("fs.chunks", 45, "files_id", 300, 1.0), F("fs.files", 5, "owner", 100, 1.0), I("fs.chunks", 50)]),
    App(74, "accorciatore", 1, "always", 40.0,
        S(links={}, clicks={"code": (20000, 1.0)}),
        [P("links", 50, 20000, 1.0), U("links", 48, 20000, 1.0), I("links", 2)]),
    App(75, "audit", 1, "always", 2.0,
        S(audit={"actor": (500, 1.0)}),
        [F("audit", 3, "actor", 500, 1.0, 0.5), I("audit", 97)]),
    App(76, "webhook", 1, "burst", 3.0,
        S(endpoints={}, deliveries={"status": (3, 0.5)}, dead_letters={}),
        [P("endpoints", 30, 200, 1.0), U("deliveries", 40, 100000, 0.0, ["status"]), I("deliveries", 25), I("dead_letters", 1)]),
    # ---- K. Generate da AI
    App(77, "todo-generato", 1, "sleep", 10.0,
        S(todos={"userId": (100, 1.0)}, users={}),
        [F("todos", 60, "userId", 100, 1.0), P("users", 10, 100, 1.0), U("todos", 15, 50000, 1.0), I("todos", 15)]),
    App(78, "ricettario-generato", 1, "sleep", 15.0,
        S(recipes={"tag": (40, 1.0)}),
        [F("recipes", 50, None, 1, 0, 0.8), F("recipes", 30, "tag", 40, 1.0), P("recipes", 15, 1000, 1.0), I("recipes", 5)]),
    App(79, "spese-generato", 1, "burst", 0.5,
        S(expenses={"userId": (20, 1.0)}, categories={}),
        [A("expenses", 30, "userId", 20, 1.0), F("expenses", 30, "userId", 20, 1.0), P("categories", 10, 15, 1.0),
         I("expenses", 25), U("expenses", 5, 3000, 1.0)]),
    App(80, "twitter-tutorial", 1, "sleep", 20.0,
        S(tweets={"userId": (100, 1.0)}, users={}, follows={"followerId": (100, 1.0)}),
        [F("tweets", 30, "userId", 100, 1.0, 0.5), P("users", 40, 100, 1.0), F("follows", 10, "followerId", 100, 1.0),
         C("tweets", 10, "userId", 100, 1.0), I("tweets", 10)]),
    App(81, "annunci-generato", 1, "burst", 2.0,
        S(listings={"area": (50, 1.0)}, images={"listingId": (5000, 1.0)}),
        [F("listings", 50, "area", 50, 1.0, 0.7), P("listings", 25, 5000, 1.0), F("images", 20, "listingId", 5000, 1.0), I("listings", 5)]),
    App(82, "app-qualsiasi", 1, "sleep", 30.0,
        S(stuff={"k": (50, 1.0)}),
        [F("stuff", 40, "k", 50, 1.0, 0.5), C("stuff", 10, None, 1, 0, 0.5), I("stuff", 50)]),
    # ---- L. Piattaforme
    App(83, "saas-200", 200, "day", 1.0,
        S(tickets={"assigneeId": (5, 0.8), "status": (4, 0.5)}, comments={"ticketId": (500, 1.0)}, agents={}),
        [F("tickets", 30, "assigneeId", 5, 0.8, 0.1), C("tickets", 15, "status", 4, 0.5), P("tickets", 20, 500, 1.0),
         F("comments", 10, "ticketId", 500, 1.0), F("tickets", 3, None, 1, 0, 0.95),
         U("tickets", 10, 500, 1.0, ["status", "assigneeId"]), I("comments", 8), I("tickets", 2)], active_frac=0.2),
    App(84, "note-500", 500, "peak", 0.0,
        S(notes={"folder": (5, 1.0)}),
        [F("notes", 40, "folder", 5, 1.0), P("notes", 40, 300, 1.0), U("notes", 15, 300, 1.0), I("notes", 5)],
        peaks=[(0, 3600, 0.05)]),  # risveglio di massa 8-9: circa 180 operazioni per database in un'ora
    App(85, "sandbox", 3, "burst", 5.0,
        S(docs={"k": (100, 1.0)}),
        [F("docs", 40, "k", 100, 1.0, 0.3), P("docs", 30, 10000, 1.0), C("docs", 10, "k", 100, 1.0), I("docs", 20)]),
    App(86, "ci", 4, "burst", 10.0,
        S(docs={"k": (100, 1.0)}),
        [F("docs", 30, "k", 100, 1.0, 0.3), P("docs", 20, 10000, 0.5), C("docs", 10, "k", 100, 1.0), I("docs", 40)]),
]

assert len(APPS) == 86, len(APPS)

DURATA = 14400  # 08:00-12:00


def bursts(rng, dur):
    """3 scatti all'ora, ognuno di 2-10 minuti."""
    out = []
    for h in range(int(math.ceil(dur / 3600))):
        for _ in range(3):
            st = h * 3600 + rng.uniform(0, 3600)
            out.append((st, st + rng.uniform(120, 600)))
    return out


def rate_fn(app, rng):
    if app.shape in ("always", "day"):
        return lambda t: app.rate
    if app.shape == "sleep":
        return lambda t: app.rate / 3600.0
    if app.shape == "burst":
        b = bursts(rng, DURATA)
        return lambda t: app.rate if any(s <= t < e for s, e in b) else 0.0
    if app.shape == "peak":
        pk = app.peaks
        base = app.rate

        def f(t):
            for s, d, m in pk:
                if s <= t < s + d:
                    return m
            return base
        return f
    raise ValueError(app.shape)


def poisson(rng, lam):
    if lam <= 0:
        return 0
    if lam > 50:
        return max(0, int(rng.gauss(lam, math.sqrt(lam)) + 0.5))
    L, k, p = math.exp(-lam), 0, 1.0
    while True:
        p *= rng.random()
        if p <= L:
            return k
        k += 1


def generate(seed=1, card_mult=1.0, z_mult=1.0, uniq_add=0.0, apps=None, durata=DURATA):
    """Produce gli eventi in ordine di tempo (risoluzione 1 s, ordine casuale dentro il secondo)."""
    rng = random.Random(seed)
    dbs = []  # (app, db_id, rate_fn, ops, weights, newid_counter)
    for app in (apps or APPS):
        n_active = max(1, round(app.ndb * app.active_frac)) if app.active_frac < 1 else app.ndb
        for d in range(app.ndb):
            active = d < n_active
            rf = rate_fn(app, rng)
            if not active:
                # dormiente: una operazione ogni ~2 ore
                rf = (lambda t: 1.0 / 7200.0)
            ops = app.ops
            w = [o[2] for o in ops]
            tot = sum(w)
            cum, acc = [], 0.0
            for x in w:
                acc += x / tot
                cum.append(acc)
            dbs.append([app, f"{app.num:02d}-{app.name}/{d}", rf, ops, cum, [0]])

    def card(c):
        return max(1, int(c * card_mult))

    def zz(z):
        return z * z_mult

    for t in range(durata):
        evs = []
        for app, db, rf, ops, cum, ctr in dbs:
            lam = rf(t)
            n = poisson(rng, lam)
            for _ in range(n):
                o = ops[min(bisect.bisect_left(cum, rng.random()), len(ops) - 1)]
                kind, coll, _, field, cc, z, extra = o
                tt = t + rng.random()
                schema = app.schema.get(coll, {})
                if kind == "point":
                    i = zipf(rng, card(cc), zz(z))
                    evs.append((tt, db, coll, "point", ("p", coll, i), [(coll, "_id", i), (coll, "_id*")]))
                elif kind in ("filter", "count", "agg"):
                    uq = min(1.0, extra + uniq_add)
                    if field is None:
                        v = None
                        toks = [(coll, "ALL")]
                    else:
                        v = zipf(rng, card(cc), zz(z))
                        toks = [(coll, field, v)]
                    variant = rng.random() if rng.random() < uq else 0
                    evs.append((tt, db, coll, kind, (kind[0], coll, field, v, variant), toks))
                elif kind == "ins":
                    ctr[0] += 1
                    toks = [(coll, "ALL")]  # il nuovo _id non è mai letto prima: nessun gettone
                    for f, (fc, fz) in schema.items():
                        toks.append((coll, f, zipf(rng, card(fc), zz(fz))))
                    evs.append((tt, db, coll, "write", None, toks))
                elif kind == "upd":
                    i = zipf(rng, card(cc), zz(z))
                    toks = [(coll, "ALL"), (coll, "_id", i)]
                    for f, (fc, fz) in schema.items():
                        toks.append((coll, f, zipf_from_u(h01(db, coll, i, f), card(fc), zz(fz))))
                    for f in extra:
                        fc, fz = schema[f]
                        toks.append((coll, f, zipf(rng, card(fc), zz(fz))))
                    evs.append((tt, db, coll, "write", None, toks))
                elif kind == "many":
                    v = zipf(rng, card(cc), zz(z))
                    toks = [(coll, "ALL"), (coll, "_id*"), (coll, field, v)]
                    evs.append((tt, db, coll, "write", None, toks))
        evs.sort(key=lambda e: e[0])
        yield from evs
