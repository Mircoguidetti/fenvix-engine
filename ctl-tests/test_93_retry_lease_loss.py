"""fix-retry-lease-loss (2026-11-14): a retryable write reported failed (134 WriteOutcomeUnknown) but applied must be
answered from the session's record when the driver retries it, also across a lease loss.

Seen on night-head 36c6a3e (test_52r::test_split_insert_many_with_driver_retries_throttled, 6/6 sealed2-derive): the first
insert of a new database, started while the process lease's renewals are held past its TTL, stages its batch (documents
and the session's record together), then the new collection's uuid write finds the database reopened by the lease loss
and is refused: the command fails before the record joins the session's memory, and its staged batch lands (the abandon
flushes the WAL). The client gets 134; the driver's retry (same lsid/txnNumber) was answered from the stale memory, ran
again and got E11000 on _id 0: told nInserted=0, every document stored.

The fix (`retry.rs`, `Drop for Run`): a command that staged its record and failed before applying it drops the session's
memory; the retry reads the record back from the database (after waiting for the staged batch, from the next opening if
that one was given up) and is answered from it.

test_retry_of_an_applied_insert_is_answered_from_the_record: what the client is told matches what is stored.
test_control_memory_kept: the same with the old behaviour (`FENVIX_TEST_CONTROL_UNAPPLIED_RECORD_KEPT=1`): the mismatch
appears (proof that the test sees the defect; on the base, where the variable does not exist, the first test fails).
Both run in any mode (FENVIX_SEALED_RUNS off or sealed2-derive).
"""
import os
import tempfile
import threading
import time

from pymongo import MongoClient, monitoring
from pymongo.errors import BulkWriteError, PyMongoError

from lib import Server, make_bucket, free_port

TTL = 2
N = 101_000   # two insert commands: 100,000 (maxWriteBatchSize) then 1,000


class _Inserts(monitoring.CommandListener):
    def __init__(self):
        self.events = []

    def started(self, e):
        pass

    def succeeded(self, e):
        if e.command_name == "insert":
            self.events.append(("reply", e.reply.get("n"), [w.get("code") for w in e.reply.get("writeErrors", [])], "retriedStmtIds" in e.reply))

    def failed(self, e):
        if e.command_name == "insert":
            self.events.append(("failed", e.failure.get("code"), e.failure.get("codeName")))


def _insert_across_a_lease_loss(tag, env):
    """Runs the scenario until the first attempt is answered 134 (the case under test; the lease loss must fall inside
    the command), at most 4 times, each on a new bucket. Returns the last run."""
    for i in range(4):
        r = _one_run(f"{tag}{i}", env)
        if any(ev[0] == "failed" and ev[1] == 134 for ev in r[3]):
            break
    return r


def _one_run(tag, env):
    ff = tempfile.NamedTemporaryFile("w", delete=False, suffix=".faults")
    ff.close()
    ff = ff.name
    port = free_port()
    srv = Server(make_bucket(tag), tag, idle=600, holder="S1", port=port, env=dict(env, MONGO_TEST_FAULTS_FILE=ff),
                 extra=["--lease-ttl-secs", str(TTL), "--replica-set", "rs0", "--advertise", f"127.0.0.1:{port}"]).start()
    lis = _Inserts()
    try:
        c = MongoClient(srv.uri, serverSelectionTimeoutMS=5000, socketTimeoutMS=120000, retryWrites=True, event_listeners=[lis])
        time.sleep(1.5)
        # no write before the call: the call creates the database. The process lease's renewals are held past its TTL.
        with open(ff, "w") as fh:
            fh.write(f"delay_put processes/S1/_lease {TTL * 1000 + 1500}\n")

        def clear():
            time.sleep(TTL + 1.0)
            open(ff, "w").close()
        threading.Thread(target=clear).start()
        try:
            r = c.d.t.insert_many([{"_id": i, "f": "x" * 20} for i in range(N)], ordered=True)
            told = len(r.inserted_ids)
        except BulkWriteError as e:
            told = e.details.get("nInserted")
        except PyMongoError as e:
            told = f"error {e}"
        time.sleep(TTL + 2)
        stored = None
        for _ in range(60):
            try:
                stored = c.d.t.count_documents({})
                break
            except PyMongoError:
                time.sleep(1)
        losses = srv.log_text().count("LEASE lost")
        return told, stored, losses, lis.events
    finally:
        srv.term()
        os.unlink(ff)


def test_retry_of_an_applied_insert_is_answered_from_the_record():
    told, stored, losses, events = _insert_across_a_lease_loss("rtll", {})
    assert losses >= 1, f"no lease loss happened: {events}"
    assert stored is not None, "the read-back count never succeeded"
    # the case under test happened: a first attempt answered with an unknown outcome, then retried
    assert any(ev[0] == "failed" and ev[1] == 134 for ev in events), f"the first attempt was not answered 134 (timing): {events}"
    # an error (outcome unknown, the driver's retry also unknown) is allowed; a wrong count is not
    if isinstance(told, int):
        assert told == stored, f"told nInserted={told}, stored {stored}: {events}"
    assert not any(ev[0] == "reply" and 11000 in ev[2] for ev in events), f"a retry got E11000 on its own first attempt: {events}"


def test_control_memory_kept():
    """Negative control: with the old behaviour, the retry of an applied first attempt gets E11000 (told != stored)."""
    told, stored, losses, events = _insert_across_a_lease_loss("rtllc", {"FENVIX_TEST_CONTROL_UNAPPLIED_RECORD_KEPT": "1"})
    assert losses >= 1, f"no lease loss happened: {events}"
    assert any(ev[0] == "failed" and ev[1] == 134 for ev in events), f"the first attempt was not answered 134 (timing): {events}"
    assert isinstance(told, int) and told != stored, f"the control did not show the defect: told {told}, stored {stored}: {events}"
