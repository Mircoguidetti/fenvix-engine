"""`$toLong` and `$convert` to long of a date (found by the agent-adapters session: Mastra seeds a run's generation with
`{$toLong: '$$NOW'}` in an update pipeline). mongod 7.0 returns the milliseconds since the epoch as a long (negative before
1970); to int and from a timestamp it stays error 241 (MEASURED on the reference mongod of `judge/setup.sh`).
Every case runs on our server and, when it is reachable on port 27020, on the reference mongod."""
import datetime

import pytest
from bson import Int64, Timestamp
from pymongo import ReturnDocument
from pymongo.errors import OperationFailure

from test_42_unique_txn import client, ours, real  # noqa: F401  (fixtures)

UTC = datetime.timezone.utc
D1 = datetime.datetime(2026, 1, 1, tzinfo=UTC)
D2 = datetime.datetime(1960, 5, 3, 1, 2, 3, 456000, tzinfo=UTC)


@pytest.fixture
def co(client):
    client.drop_database("tolong")
    c = client.tolong.t
    c.insert_many([{"_id": 1, "d": D1}, {"_id": 2, "d": D2}, {"_id": 3, "d": Timestamp(5, 1)}])
    return c


def _x(co, i, e):
    return co.aggregate([{"$match": {"_id": i}}, {"$project": {"_id": 0, "x": e}}]).next()["x"]


@pytest.mark.parametrize("e", [{"$toLong": "$d"}, {"$convert": {"input": "$d", "to": "long"}}, {"$convert": {"input": "$d", "to": 18}},
                               {"$convert": {"input": "$d", "to": "long", "onError": "E"}}])
def test_date_to_long(co, e):
    for i, want in ((1, 1767225600000), (2, -304988276544)):
        x = _x(co, i, e)
        assert x == want and isinstance(x, Int64), (i, x)


def test_date_to_int_and_timestamp_to_long_stay_errors(co):
    for i, e in ((1, {"$toInt": "$d"}), (3, {"$toLong": "$d"})):
        with pytest.raises(OperationFailure) as ex:
            _x(co, i, e)
        assert ex.value.code == 241
    assert _x(co, 1, {"$convert": {"input": "$d", "to": "int", "onError": "E"}}) == "E"
    assert _x(co, 3, {"$convert": {"input": "$d", "to": "long", "onError": "E"}}) == "E"


def test_mastra_generation_seed(co):
    r = co.find_one_and_update({"_id": "run1", "generation": {"$exists": False}},
                               [{"$set": {"generation": {"$max": [1, {"$toLong": "$$NOW"}]}}}], upsert=True, return_document=ReturnDocument.AFTER)
    assert isinstance(r["generation"], Int64) and r["generation"] > 1_700_000_000_000
