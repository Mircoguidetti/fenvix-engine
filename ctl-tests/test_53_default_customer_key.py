"""integration-4, prompt P-integration-4 section 3b: the LangGraph saver's collections (`checkpoints`, `checkpoint_writes`)
are customer-keyed by `thread_id` by default, so derived documents work without `--customer-key`; any `--customer-key`
for the same collection given on the command line still wins.

How it is seen from a client: in a customer-keyed collection `_id` is unique per customer (founder, 2026-11-07), so two
documents with the same `_id` and different `thread_id` are both accepted; in a plain collection the second is a
duplicate key (11000).
  - default: both accepted in `checkpoints` and in `checkpoint_writes`; a collection not in the list (`other`) refuses;
  - `--customer-key checkpoints=user_id`: in `checkpoints` the key is user_id (same user_id: refused even with different
    thread_ids), while `checkpoint_writes` keeps the default thread_id;
  - `--agent-checkpoints ""`: no default at all."""
import pytest
from pymongo.errors import DuplicateKeyError

from lib import Server, make_bucket


def two_threads_same_id(coll):
    """True when two documents with _id 1 and different thread_id are both stored (customer-keyed by thread_id)."""
    coll.insert_one({"_id": 1, "thread_id": "t-a", "user_id": "u"})
    try:
        coll.insert_one({"_id": 1, "thread_id": "t-b", "user_id": "u"})
    except DuplicateKeyError:
        return False
    return coll.count_documents({"_id": 1}) == 2


def run(extra):
    s = Server(make_bucket("defkey"), "defkey", idle=600, extra=extra).start()
    try:
        db = s.client().app
        return {c: two_threads_same_id(db[c]) for c in ("checkpoints", "checkpoint_writes", "other")}
    finally:
        s.term()


def test_saver_collections_keyed_by_thread_id_by_default():
    assert run([]) == {"checkpoints": True, "checkpoint_writes": True, "other": False}


def test_command_line_wins():
    assert run(["--customer-key", "checkpoints=user_id"]) == {"checkpoints": False, "checkpoint_writes": True, "other": False}


def test_no_agent_collections_no_default():
    assert run(["--agent-checkpoints", ""]) == {"checkpoints": False, "checkpoint_writes": False, "other": False}
