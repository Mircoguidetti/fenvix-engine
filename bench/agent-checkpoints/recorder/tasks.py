"""Part C · the public-style tasks the recorded agents work on, and the tools they call. All data is generated here
(RULES.md 2026-11-09: no customer data). Two families, the two shapes of agent apps that keep every checkpoint:
  support  a retail customer-support agent in the style of tau-bench's retail domain (orders, returns, address
           changes): a generated store of customers and orders; the user's turns are SCRIPTED per task (tau-bench uses
           a second LLM as the user; we do not pay for one);
  coding   a coding agent in the style of SWE-bench, on small generated Python repositories: one function with a bug
           and a failing test; tools list/read/write files and run the tests in a temporary folder (subprocess, 20 s).
`make_task(kind, i, seed)` returns a Task: system prompt, scripted user turns, tool definitions (Anthropic tool schema,
strict), and a `call(name, input)` that runs a tool and returns its text."""
from __future__ import annotations

import json
import os
import random
import subprocess
import sys
import tempfile
from dataclasses import dataclass, field

SUPPORT_SYSTEM = ("You are the customer-support agent of an online store. Use the tools to look up customers and "
                  "orders before answering; never invent order data. Refunds only for delivered orders within 30 days. "
                  "Confirm every change with the user before making it. Be brief.")
CODING_SYSTEM = ("You are a coding agent working in a small Python repository. Find and fix the bug that makes the tests "
                 "fail. Read the relevant files, make the smallest change, run the tests, and report what you changed.")


def _tool(name, desc, props, required):
    return {"name": name, "description": desc, "strict": True,
            "input_schema": {"type": "object", "properties": props, "required": required, "additionalProperties": False}}


SUPPORT_TOOLS = [
    _tool("find_customer", "Find a customer by email.", {"email": {"type": "string"}}, ["email"]),
    _tool("list_orders", "List a customer's orders (id, date, status, total).", {"customer_id": {"type": "string"}}, ["customer_id"]),
    _tool("get_order", "Full details of one order: items, prices, status, delivery date, address.", {"order_id": {"type": "string"}}, ["order_id"]),
    _tool("refund_order", "Refund a delivered order. Returns the refund id.", {"order_id": {"type": "string"}, "reason": {"type": "string"}}, ["order_id", "reason"]),
    _tool("update_address", "Change the delivery address of an order that has not shipped.",
          {"order_id": {"type": "string"}, "address": {"type": "string"}}, ["order_id", "address"]),
]
CODING_TOOLS = [
    _tool("list_files", "List the files of the repository.", {}, []),
    _tool("read_file", "Read a file of the repository.", {"path": {"type": "string"}}, ["path"]),
    _tool("write_file", "Replace the whole content of a file.", {"path": {"type": "string"}, "content": {"type": "string"}}, ["path", "content"]),
    _tool("run_tests", "Run the test suite and return its output.", {}, []),
]

ITEMS = ["desk lamp", "water bottle", "running shoes", "backpack", "headphones", "coffee grinder", "yoga mat", "kettle",
         "notebook set", "phone case", "rain jacket", "cutting board", "wall clock", "bike light", "travel mug"]
STATUSES = ["processing", "shipped", "delivered", "delivered", "delivered", "cancelled"]

BUGS = [  # (module, function source with a bug, the fix applied by the oracle, test)
    ("pricing.py", "def total(prices, discount):\n    s = sum(prices)\n    return s - s * discount / 10\n",
     "def total(prices, discount):\n    s = sum(prices)\n    return s - s * discount / 100\n",
     "from pricing import total\n\ndef test_total():\n    assert total([10, 20, 70], 10) == 90\n"),
    ("dates.py", "def is_leap(y):\n    return y % 4 == 0 and y % 100 != 0\n",
     "def is_leap(y):\n    return y % 4 == 0 and (y % 100 != 0 or y % 400 == 0)\n",
     "from dates import is_leap\n\ndef test_leap():\n    assert is_leap(2000) and not is_leap(1900) and is_leap(2024)\n"),
    ("text.py", "def words(s):\n    return s.split(' ')\n",
     "def words(s):\n    return s.split()\n",
     "from text import words\n\ndef test_words():\n    assert words('a  b\\tc') == ['a', 'b', 'c']\n"),
    ("stats.py", "def median(xs):\n    xs = sorted(xs)\n    return xs[len(xs) // 2]\n",
     "def median(xs):\n    xs = sorted(xs)\n    n = len(xs)\n    return xs[n // 2] if n % 2 else (xs[n // 2 - 1] + xs[n // 2]) / 2\n",
     "from stats import median\n\ndef test_median():\n    assert median([3, 1, 2]) == 2 and median([4, 1, 3, 2]) == 2.5\n"),
]


@dataclass
class Task:
    kind: str
    task_id: str
    system: str
    tools: list
    user_turns: list
    state: dict = field(default_factory=dict)

    def call(self, name: str, inp: dict) -> str:
        return (_support_call if self.kind == "support" else _coding_call)(self, name, inp)


def _store(rng: random.Random, n_customers=40):
    customers, orders = {}, {}
    for c in range(n_customers):
        cid = f"C{c:04d}"
        customers[f"user{c}@example.com"] = {"customer_id": cid, "name": f"Customer {c}", "since": f"20{rng.randint(18, 25)}"}
        for k in range(rng.randint(2, 9)):
            oid = f"O{c:04d}{k:02d}"
            items = [{"item": rng.choice(ITEMS), "qty": rng.randint(1, 3), "price": round(rng.uniform(5, 180), 2)}
                     for _ in range(rng.randint(1, 4))]
            orders[oid] = {"order_id": oid, "customer_id": cid, "date": f"2026-{rng.randint(6, 11):02d}-{rng.randint(1, 28):02d}",
                           "status": rng.choice(STATUSES), "items": items, "address": f"{rng.randint(1, 200)} Main Street, Dublin",
                           "total": round(sum(i["qty"] * i["price"] for i in items), 2)}
    return customers, orders


def _support_call(t: Task, name: str, inp: dict) -> str:
    customers, orders = t.state["customers"], t.state["orders"]
    if name == "find_customer":
        return json.dumps(customers.get(inp["email"], {"error": "no customer with this email"}))
    if name == "list_orders":
        return json.dumps([{k: o[k] for k in ("order_id", "date", "status", "total")} for o in orders.values()
                           if o["customer_id"] == inp["customer_id"]])
    if name == "get_order":
        return json.dumps(orders.get(inp["order_id"], {"error": "no such order"}))
    if name == "refund_order":
        o = orders.get(inp["order_id"])
        if not o or o["status"] != "delivered":
            return json.dumps({"error": "only delivered orders can be refunded"})
        o["status"] = "refunded"
        return json.dumps({"refund_id": "R" + inp["order_id"][1:], "amount": o["total"]})
    if name == "update_address":
        o = orders.get(inp["order_id"])
        if not o or o["status"] != "processing":
            return json.dumps({"error": "the address can only change before shipping"})
        o["address"] = inp["address"]
        return json.dumps({"ok": True})
    return json.dumps({"error": f"unknown tool {name}"})


def _coding_call(t: Task, name: str, inp: dict) -> str:
    root = t.state["root"]
    safe = lambda p: os.path.join(root, os.path.basename(p))  # flat repository: no path outside the folder
    if name == "list_files":
        return "\n".join(sorted(os.listdir(root)))
    if name == "read_file":
        p = safe(inp["path"])
        return open(p).read() if os.path.exists(p) else f"no such file: {inp['path']}"
    if name == "write_file":
        open(safe(inp["path"]), "w").write(inp["content"])
        return "written"
    if name == "run_tests":
        r = subprocess.run([sys.executable, "-m", "pytest", "-q", "-p", "no:cacheprovider"], cwd=root,
                           capture_output=True, text=True, timeout=20)
        return (r.stdout + r.stderr)[-6000:]
    return f"unknown tool {name}"


def make_task(kind: str, i: int, seed: int) -> Task:
    rng = random.Random(seed * 1000 + i)
    if kind == "support":
        customers, orders = _store(rng)
        email = f"user{rng.randrange(40)}@example.com"
        asks = [f"Hi, I'm {email}. Can you tell me the status of my latest order?",
                "I'd like a refund for any delivered order with headphones or a kettle, it arrived damaged.",
                "Also, for any order still processing, please change the address to 12 Harbour Road, Galway.",
                "Yes, go ahead with those changes.", "What is the total I will get back?", "Thanks, that's all."]
        n = rng.randint(3, len(asks))
        return Task("support", f"support-{seed}-{i}", SUPPORT_SYSTEM, SUPPORT_TOOLS, asks[:n],
                    {"customers": customers, "orders": orders})
    mod, buggy, _, test = BUGS[i % len(BUGS)]
    root = tempfile.mkdtemp(prefix="repo-")
    open(os.path.join(root, mod), "w").write(buggy)
    open(os.path.join(root, f"test_{mod}"), "w").write(test)
    open(os.path.join(root, "README.md"), "w").write(f"# small repo {i}\n\nRun the tests with pytest.\n")
    asks = ["The tests are failing in this repository. Please find and fix the bug.",
            "Can you explain the root cause in two sentences?"]
    return Task("coding", f"coding-{seed}-{i}", CODING_SYSTEM, CODING_TOOLS, asks, {"root": root, "fix": BUGS[i % len(BUGS)][2]})
