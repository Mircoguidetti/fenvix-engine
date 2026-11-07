"""Sanity tests: python3 test_draft_runs.py (needs the tau2 checkout for the initial databases)."""
import unittest

import numpy as np

import agent_runs as ar
import collisions as co
import docs_model as dm


def run(calls, t0=0.0, t1=100.0):
    return {"calls": calls, "t0": t0, "t1": t1, "gaps": [t1 - t0], "agent_gaps": [t1 - t0]}


class Retail(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.db = dm.DB("retail")
        orders = cls.db.tables["orders"]
        cls.gift = next(o for o, r in orders.items() if r.get("status") == "pending" and any(
            p.get("payment_method_id", "").startswith("gift_card") for p in r.get("payment_history") or []))
        cls.user = orders[cls.gift]["user_id"]
        cls.other = next(o for o, r in orders.items() if r["user_id"] == cls.user and o != cls.gift)
        cls.user2 = next(r["user_id"] for r in orders.values() if r["user_id"] != cls.user)
        cls.order2 = next(o for o, r in orders.items() if r["user_id"] == cls.user2)

    def test_cancel_with_gift_card_writes_order_and_user(self):
        docs, _ = dm.Mapper(self.db).writes("cancel_pending_order", {"order_id": self.gift, "reason": "x"})
        self.assertEqual({d.key for d in docs}, {("orders", self.gift), ("users", self.user)})
        self.assertTrue(all(d.owner == self.user for d in docs))

    def test_read_own_write_is_detected(self):
        r = run([(1, "r", "get_user_details", {"user_id": self.user}, ""),
                 (2, "w", "cancel_pending_order", {"order_id": self.gift, "reason": "x"}, ""),
                 (3, "r", "get_order_details", {"order_id": self.gift}, "")])
        m = ar.measure(r, self.db)
        self.assertTrue(m["ryw_doc"])
        self.assertTrue(m["multi_doc_within_call"])
        self.assertFalse(m["multi_doc_cross_calls"])
        self.assertEqual(m["n_customers"], 1)

    def test_read_of_other_document_is_customer_level_only(self):
        r = run([(1, "w", "modify_pending_order_address", {"order_id": self.gift}, ""),
                 (2, "r", "get_order_details", {"order_id": self.other}, "")])
        m = ar.measure(r, self.db)
        self.assertFalse(m["ryw_doc"])
        self.assertTrue(m["ryw_cust"])

    def test_two_customers(self):
        r = run([(1, "w", "modify_pending_order_address", {"order_id": self.gift}, ""),
                 (2, "w", "modify_pending_order_address", {"order_id": self.order2}, "")])
        m = ar.measure(r, self.db)
        self.assertEqual(m["n_customers"], 2)
        self.assertTrue(m["multi_doc_cross_calls"])

    def test_log_writes_excluded_in_core(self):
        r = run([(1, "w", "modify_user_address", {"user_id": self.user}, ""),
                 (2, "w", "log_verification", {"user_id": self.user}, "")])
        self.assertEqual(ar.measure(r, self.db, exclude=ar.LOG_WRITES)["n_writes"], 1)


class Airline(unittest.TestCase):
    def test_booking_touches_shared_flight(self):
        db = dm.DB("airline")
        u = next(iter(db.tables["users"]))
        fn = next(iter(db.tables["flights"]))
        r = run([(1, "w", "book_reservation", {"user_id": u, "flights": [{"flight_number": fn, "date": "x"}],
                                               "payment_methods": []}, ""),
                 (2, "r", "search_direct_flight", {"origin": db.tables["flights"][fn]["origin"],
                                                   "destination": db.tables["flights"][fn]["destination"]}, "")])
        m = ar.measure(r, db)
        self.assertIn(f"flights/{fn}", m["shared_written"])
        self.assertTrue(m["ryw_doc"])  # the search reads the seat count the booking changed
        self.assertEqual(m["n_customers"], 1)


class Banking(unittest.TestCase):
    def test_query_by_account_finds_written_card(self):
        db = dm.DB("banking_knowledge")
        card, rec = next((c, r) for c, r in db.tables["debit_cards"].items() if r.get("account_id"))
        r = run([(1, "w", "freeze_debit_card_3892", {"card_id": card}, ""),
                 (2, "r", "get_debit_cards_by_account_id_7823", {"account_id": rec["account_id"]}, "")])
        self.assertTrue(ar.measure(r, db)["ryw_doc"])


class Swe(unittest.TestCase):
    def test_write_commands(self):
        self.assertEqual(ar.swe_writes("sed -i 's/a/b/' astropy/modeling/separable.py"),
                         {"astropy/modeling/separable.py"})
        self.assertEqual(ar.swe_writes("cat <<'EOF' > reproduce.py\nprint(1)\nEOF"), {"reproduce.py"})
        self.assertEqual(ar.swe_writes("echo x >> /testbed/a/b.py"), {"a/b.py"})
        self.assertEqual(ar.swe_writes("python -c \"open('x.txt','w').write('1')\""), {"x.txt"})
        self.assertEqual(ar.swe_writes("grep -n foo a/b.py 2>/dev/null"), set())
        self.assertEqual(ar.swe_writes("cat a/b.py"), set())


class Collisions(unittest.TestCase):
    def test_busy_user_always_collides_and_lone_event_never(self):
        day = 86400.0
        busy_t = np.arange(0, day, 10.0)                      # an event every 10 s all day
        codes = np.concatenate([np.zeros(len(busy_t), dtype=np.int64), np.ones(1, dtype=np.int64)])
        t = np.concatenate([busy_t, [day / 2]])               # user 1: one event that day
        order = np.lexsort((t, codes))
        res = co.collision_probs(codes[order], t[order], [60.0], samples_per_day=2000)
        # pooled over the 2 active user-days: busy ~100%, lone ~0% (only the moments just before its event)
        self.assertAlmostEqual(res["by_L"]["60.0"]["uniform_moment"], 50.0, delta=1.5)
        # event anchor: every busy event but the last has a next one 10 s later; the lone event has none
        n = len(busy_t) + 1
        self.assertAlmostEqual(res["by_L"]["60.0"]["event_anchor"], 100.0 * (n - 2) / n, delta=0.01)

    def test_uniform_matches_rate(self):
        # one event per hour, L = 30 min: a random moment has the next event within L half of the time
        t = np.arange(0, 86400.0 * 3, 3600.0)
        codes = np.zeros(len(t), dtype=np.int64)
        res = co.collision_probs(codes, t, [1800.0], samples_per_day=5000)
        self.assertAlmostEqual(res["by_L"]["1800.0"]["uniform_moment"], 50.0, delta=2.0)
        self.assertEqual(res["by_L"]["1800.0"]["event_anchor"], 0.0)


if __name__ == "__main__":
    unittest.main()
