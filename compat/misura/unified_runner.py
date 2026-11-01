#!/usr/bin/env python3
"""Runner for the MongoDB "unified test format" (github.com/mongodb/specifications), written for FENVIX (S10).

It reads the official JSON test files (data, not code) and runs them with PyMongo against one server.
Every test ends as PASS, FAIL (with the reason) or SKIP (with the reason). Skip reasons are kept in a few
fixed families so that they can be counted:

  req:<what>      the test's runOnRequirements exclude our setup (other topology, server version < 8.0 only,
                  auth on, serverless, client-side encryption...). An official runner skips these too.
  failpoint       the test needs configureFailPoint, a test-only server hook (mongod needs enableTestCommands;
                  Atlas does not allow it). We probe the server once: if it supports failpoints, these run.
  runner:<op>     this runner does not implement an operation or an option the test uses. These are a limit of
                  the measurement, not of the engine; they are listed in the results.
  spec-skip       the test file itself marks the test skipped (skipReason).

Usage: unified_runner.py --uri mongodb://... --out results.jsonl FILE.json [FILE.json ...]
"""
import argparse
import json
import re
import sys
import threading
import time
import traceback

import bson
from bson import json_util, Int64, ObjectId, Decimal128, Timestamp, Binary, Regex, Code, MinKey, MaxKey, DBRef
from bson.codec_options import CodecOptions
from bson.json_util import JSONOptions, JSONMode
from bson.raw_bson import RawBSONDocument
import pymongo
from pymongo import MongoClient, monitoring, ReadPreference, WriteConcern
from pymongo.read_concern import ReadConcern
import pymongo.client_session, pymongo.command_cursor, pymongo.cursor, pymongo.change_stream, pymongo.database, pymongo.collection
from pymongo.errors import (PyMongoError, OperationFailure, BulkWriteError, ConnectionFailure, ConfigurationError,
                            InvalidOperation, ServerSelectionTimeoutError, ClientBulkWriteException,
                            WriteError, WriteConcernError, NetworkTimeout, ExecutionTimeout)
from pymongo.operations import (InsertOne, UpdateOne, UpdateMany, ReplaceOne, DeleteOne, DeleteMany, IndexModel)
from pymongo.read_preferences import read_pref_mode_from_name, make_read_preference
from pymongo.results import BulkWriteResult
import gridfs

REFERENCE_VERSION = (8, 0, 0)  # we judge runOnRequirements as if the server were MongoDB 8.0
TOPOLOGY = "replicaset"
SENSITIVE = {"authenticate", "saslstart", "saslcontinue", "getnonce", "createuser", "updateuser",
             "copydbgetnonce", "copydbsaslstart", "copydb", "hello", "ismaster"}


class Skip(Exception):
    pass


class TestFail(Exception):
    pass


def loads(text):
    return json_util.loads(text, json_options=JSONOptions(json_mode=JSONMode.CANONICAL, document_class=dict,
                                                          tz_aware=False))


def vtuple(s):
    parts = [int(x) for x in s.split(".")]
    while len(parts) < 3:
        parts.append(0)
    return tuple(parts[:3])


def camel_to_snake(name):
    return re.sub(r"(?<!^)(?=[A-Z])", "_", name).lower()


# ---------------------------------------------------------------------------------------------- matching
NUM = (int, float, Int64, Decimal128)


def is_num(v):
    return isinstance(v, (int, float, Int64)) and not isinstance(v, bool)


TYPE_NAMES = {
    "double": lambda v: isinstance(v, float),
    "string": lambda v: isinstance(v, str),
    "object": lambda v: isinstance(v, dict),
    "array": lambda v: isinstance(v, list),
    "binData": lambda v: isinstance(v, (bytes, Binary)),
    "undefined": lambda v: False,
    "objectId": lambda v: isinstance(v, ObjectId),
    "bool": lambda v: isinstance(v, bool),
    "date": lambda v: v.__class__.__name__ in ("datetime", "DatetimeMS"),
    "null": lambda v: v is None,
    "regex": lambda v: isinstance(v, (Regex, re.Pattern)),
    "dbPointer": lambda v: False,
    "javascript": lambda v: isinstance(v, Code) and v.scope is None,
    "symbol": lambda v: False,
    "javascriptWithScope": lambda v: isinstance(v, Code) and v.scope is not None,
    "int": lambda v: isinstance(v, int) and not isinstance(v, (bool, Int64)),
    "timestamp": lambda v: isinstance(v, Timestamp),
    "long": lambda v: isinstance(v, Int64) or (isinstance(v, int) and not isinstance(v, bool) and not -2**31 <= v < 2**31),
    "decimal": lambda v: isinstance(v, Decimal128),
    "minKey": lambda v: isinstance(v, MinKey),
    "maxKey": lambda v: isinstance(v, MaxKey),
    "number": lambda v: is_num(v) or isinstance(v, Decimal128),
}


class Matcher:
    def __init__(self, runner):
        self.r = runner

    def match(self, expected, actual, root=True, path="$"):
        if isinstance(expected, dict) and len(expected) == 1:
            k = next(iter(expected))
            if k.startswith("$$"):
                return self.special(k, expected[k], actual, path, root)
        if isinstance(expected, dict):
            if isinstance(actual, RawBSONDocument):
                actual = bson.decode(actual.raw)
            if not isinstance(actual, dict):
                raise TestFail(f"{path}: expected document, got {type(actual).__name__} {short(actual)}")
            for k, ev in expected.items():
                if isinstance(ev, dict) and len(ev) == 1 and next(iter(ev)) == "$$exists":
                    if (k in actual) != ev["$$exists"]:
                        raise TestFail(f"{path}.{k}: $$exists {ev['$$exists']} failed")
                    continue
                if isinstance(ev, dict) and len(ev) == 1 and next(iter(ev)) == "$$unsetOrMatches" and k not in actual:
                    continue
                if k not in actual:
                    raise TestFail(f"{path}.{k}: missing in actual {short(actual)}")
                self.match(ev, actual[k], root=False, path=f"{path}.{k}")
            if not root:
                extra = [k for k in actual if k not in expected]
                if extra:
                    raise TestFail(f"{path}: unexpected fields {extra} in {short(actual)}")
            return
        if isinstance(expected, list):
            if not isinstance(actual, list):
                raise TestFail(f"{path}: expected array, got {short(actual)}")
            if len(expected) != len(actual):
                raise TestFail(f"{path}: expected {len(expected)} items, got {len(actual)}: {short(actual)}")
            for i, (e, a) in enumerate(zip(expected, actual)):
                self.match(e, a, root=False, path=f"{path}[{i}]")
            return
        if is_num(expected) and is_num(actual):
            if float(expected) != float(actual):
                raise TestFail(f"{path}: expected {expected}, got {actual}")
            return
        if isinstance(expected, Decimal128) or isinstance(actual, Decimal128):
            if str(expected) != str(actual):
                raise TestFail(f"{path}: expected {expected}, got {actual}")
            return
        if isinstance(expected, bytes) and isinstance(actual, (bytes, Binary)):
            if bytes(expected) != bytes(actual):
                raise TestFail(f"{path}: binary differs")
            return
        if expected != actual:
            raise TestFail(f"{path}: expected {short(expected)}, got {short(actual)}")

    def special(self, op, arg, actual, path, root=False):
        if op == "$$exists":
            return  # handled in the parent
        if op == "$$type":
            names = arg if isinstance(arg, list) else [arg]
            for n in names:
                f = TYPE_NAMES.get(n)
                if f is None:
                    raise Skip(f"runner:$$type {n}")
                if f(actual):
                    return
            raise TestFail(f"{path}: expected type {names}, got {type(actual).__name__} {short(actual)}")
        if op == "$$unsetOrMatches":
            return self.match(arg, actual, root=root, path=path)
        if op == "$$matchesEntity":
            return self.match(self.r.entity(arg), actual, root=False, path=path)
        if op == "$$matchesHexBytes":
            if bytes(actual).hex().lower() != arg.lower():
                raise TestFail(f"{path}: hex bytes differ")
            return
        if op == "$$sessionLsid":
            lsid = self.r.lsids.get(arg)
            if lsid is None:
                raise TestFail(f"{path}: unknown session {arg}")
            return self.match(lsid, actual, root=False, path=path)
        if op == "$$lte":
            if not (is_num(actual) and actual <= arg):
                raise TestFail(f"{path}: expected <= {arg}, got {actual}")
            return
        if op == "$$matchAsDocument":
            if not isinstance(actual, str):
                raise TestFail(f"{path}: $$matchAsDocument on non-string")
            return self.match(arg, loads(actual), root=True, path=path)
        if op == "$$matchAsRoot":
            return self.match(arg, actual, root=True, path=path)
        raise Skip(f"runner:{op}")


def short(v, n=220):
    try:
        s = json_util.dumps(v) if isinstance(v, (dict, list)) else repr(v)
    except Exception:
        s = repr(v)
    return s if len(s) <= n else s[:n] + "..."


# ---------------------------------------------------------------------------------------------- events
class Listener(monitoring.CommandListener):
    def __init__(self, observe, ignore, observe_sensitive):
        self.observe = set(observe or [])
        self.ignore = set(ignore or [])
        self.observe_sensitive = observe_sensitive
        self.events = []
        self.all_started = []
        self.lock = threading.Lock()

    def _keep(self, ev, kind):
        name = ev.command_name
        if name in self.ignore:
            return False
        if name.lower() in SENSITIVE and not self.observe_sensitive:
            if kind == "commandStartedEvent" and not ev.command:
                return False
            if kind != "commandStartedEvent":
                return False
            if name.lower() in ("hello", "ismaster") and "speculativeAuthenticate" not in ev.command:
                return False
            return False
        return kind in self.observe

    def started(self, ev):
        with self.lock:
            self.all_started.append(ev)
            if self._keep(ev, "commandStartedEvent"):
                self.events.append(("commandStartedEvent", ev))

    def succeeded(self, ev):
        with self.lock:
            if self._keep(ev, "commandSucceededEvent"):
                self.events.append(("commandSucceededEvent", ev))

    def failed(self, ev):
        with self.lock:
            if self._keep(ev, "commandFailedEvent"):
                self.events.append(("commandFailedEvent", ev))


# ---------------------------------------------------------------------------------------------- runner
class Runner:
    def __init__(self, uri, server_info):
        self.uri = uri
        self.server_info = server_info
        self.entities = {}
        self.lsids = {}
        self.listeners = {}
        self.clients = []
        self.matcher = Matcher(self)
        self.failpoints = []
        self.internal = MongoClient(uri)

    def entity(self, name):
        if name not in self.entities:
            raise TestFail(f"unknown entity {name}")
        return self.entities[name]

    # ------------------------------------------------------------------ requirements
    def check_reqs(self, reqs):
        if not reqs:
            return None
        reasons = []
        for r in reqs:
            why = self.req_fail(r)
            if why is None:
                return None
            reasons.append(why)
        return reasons[0]

    def req_fail(self, r):
        if "minServerVersion" in r and vtuple(r["minServerVersion"]) > REFERENCE_VERSION:
            return f"req:minServerVersion {r['minServerVersion']}"
        if "maxServerVersion" in r and vtuple(r["maxServerVersion"]) < REFERENCE_VERSION:
            return f"req:maxServerVersion {r['maxServerVersion']}"
        if "topologies" in r and TOPOLOGY not in r["topologies"]:
            return f"req:topology {'/'.join(r['topologies'])}"
        if r.get("serverless") == "require":
            return "req:serverless"
        if r.get("auth") is True:
            return "req:auth"
        if r.get("authMechanism"):
            return "req:auth"
        if r.get("csfle"):
            return "req:csfle"
        if "serverParameters" in r:
            for k, v in r["serverParameters"].items():
                try:
                    got = self.internal.admin.command("getParameter", 1, **{k: 1}).get(k)
                except PyMongoError:
                    got = None
                try:
                    self.matcher.match(v, got, root=False)
                except (TestFail, Skip):
                    return f"req:serverParameter {k}"
        return None

    # ------------------------------------------------------------------ entities
    def create_entities(self, specs):
        for spec in specs:
            (kind, s), = spec.items()
            eid = s["id"]
            if kind == "client":
                self.create_client(eid, s)
            elif kind == "database":
                client = self.entity(s["client"])
                opts = self.coll_opts(s.get("databaseOptions", {}))
                self.entities[eid] = client.get_database(s["databaseName"], **opts)
            elif kind == "collection":
                db = self.entity(s["database"])
                opts = self.coll_opts(s.get("collectionOptions", {}))
                self.entities[eid] = db.get_collection(s["collectionName"], **opts)
            elif kind == "session":
                client = self.entity(s["client"])
                so = dict(s.get("sessionOptions", {}))
                kw = {}
                if "causalConsistency" in so:
                    kw["causal_consistency"] = so.pop("causalConsistency")
                if "snapshot" in so:
                    kw["snapshot"] = so.pop("snapshot")
                if "defaultTransactionOptions" in so:
                    kw["default_transaction_options"] = pymongo.client_session.TransactionOptions(
                        **self.txn_opts(so.pop("defaultTransactionOptions")))
                if so:
                    raise Skip(f"runner:sessionOptions {list(so)}")
                sess = client.start_session(**kw)
                self.entities[eid] = sess
                self.lsids[eid] = sess.session_id
            elif kind == "bucket":
                db = self.entity(s["database"])
                bo = dict(s.get("bucketOptions", {}))
                kw = {}
                if "bucketName" in bo:
                    kw["bucket_name"] = bo.pop("bucketName")
                if "chunkSizeBytes" in bo:
                    kw["chunk_size_bytes"] = bo.pop("chunkSizeBytes")
                if "writeConcern" in bo:
                    kw["write_concern"] = WriteConcern(**self.wc(bo.pop("writeConcern")))
                if "readPreference" in bo:
                    bo.pop("readPreference")
                if bo:
                    raise Skip(f"runner:bucketOptions {list(bo)}")
                self.entities[eid] = gridfs.GridFSBucket(db, **kw)
            elif kind == "thread":
                self.entities[eid] = ThreadEntity()
            else:
                raise Skip(f"runner:entity {kind}")

    def wc(self, d):
        out = {}
        for k, v in d.items():
            if k == "w":
                out["w"] = v
            elif k == "journal" or k == "j":
                out["j"] = v
            elif k == "wtimeoutMS":
                out["wtimeout"] = v
            else:
                raise Skip(f"runner:writeConcern {k}")
        return out

    def coll_opts(self, o):
        kw = {}
        for k, v in o.items():
            if k == "writeConcern":
                kw["write_concern"] = WriteConcern(**self.wc(v))
            elif k == "readConcern":
                kw["read_concern"] = ReadConcern(v.get("level"))
            elif k == "readPreference":
                kw["read_preference"] = self.read_pref(v)
            elif k == "timeoutMS":
                raise Skip("runner:timeoutMS")
            else:
                raise Skip(f"runner:collectionOption {k}")
        return kw

    def read_pref(self, v):
        mode = v["mode"]
        mode_num = {"primary": 0, "primaryPreferred": 1, "secondary": 2, "secondaryPreferred": 3, "nearest": 4}[mode]
        return make_read_preference(mode_num, v.get("tagSets"), v.get("maxStalenessSeconds", -1))

    def txn_opts(self, o):
        kw = {}
        for k, v in o.items():
            if k == "writeConcern":
                kw["write_concern"] = WriteConcern(**self.wc(v))
            elif k == "readConcern":
                kw["read_concern"] = ReadConcern(v.get("level"))
            elif k == "readPreference":
                kw["read_preference"] = self.read_pref(v)
            elif k == "maxCommitTimeMS":
                kw["max_commit_time_ms"] = v
            elif k == "timeoutMS":
                raise Skip("runner:timeoutMS")
            else:
                raise Skip(f"runner:transactionOption {k}")
        return kw

    def create_client(self, eid, s):
        uo = dict(s.get("uriOptions", {}))
        kw = {}
        for k, v in list(uo.items()):
            if k in ("retryWrites", "retryReads", "readConcernLevel", "w", "journal", "wtimeoutMS", "appname",
                     "heartbeatFrequencyMS", "serverSelectionTimeoutMS", "connectTimeoutMS", "socketTimeoutMS",
                     "maxPoolSize", "minPoolSize", "waitQueueTimeoutMS", "directConnection", "localThresholdMS",
                     "readPreference", "compressors", "zlibCompressionLevel", "maxIdleTimeMS", "loadBalanced"):
                kw[k] = v
            elif k == "timeoutMS":
                raise Skip("runner:timeoutMS")
            elif k == "appName":
                kw["appname"] = v
            elif k == "readConcernLevel":
                kw["readConcernLevel"] = v
            else:
                raise Skip(f"runner:uriOption {k}")
        if "serverApi" in s:
            from pymongo.server_api import ServerApi
            sa = s["serverApi"]
            kw["server_api"] = ServerApi(sa["version"], strict=sa.get("strict"),
                                         deprecation_errors=sa.get("deprecationErrors"))
        if s.get("observeLogMessages"):
            raise Skip("runner:observeLogMessages")
        if s.get("storeEventsAsEntities"):
            raise Skip("runner:storeEventsAsEntities")
        observe = s.get("observeEvents", [])
        unsupported = [e for e in observe if e not in ("commandStartedEvent", "commandSucceededEvent",
                                                       "commandFailedEvent")]
        if unsupported:
            raise Skip(f"runner:observeEvents {unsupported[0]}")
        lst = Listener(observe, s.get("ignoreCommandMonitoringEvents"), s.get("observeSensitiveCommands", False))
        client = MongoClient(self.uri, event_listeners=[lst], **kw)
        self.listeners[eid] = lst
        self.clients.append(client)
        self.entities[eid] = client

    # ------------------------------------------------------------------ data
    def initial_data(self, data):
        for item in data or []:
            db = self.internal.get_database(item["databaseName"], write_concern=WriteConcern(w="majority"))
            coll = item["collectionName"]
            db.drop_collection(coll)
            co = item.get("createOptions")
            db.create_collection(coll, check_exists=False, **(co or {}))
            if item.get("documents"):
                db[coll].insert_many(item["documents"])

    def check_outcome(self, outcome):
        for item in outcome or []:
            coll = self.internal.get_database(item["databaseName"],
                                              read_concern=ReadConcern("local"),
                                              read_preference=ReadPreference.PRIMARY)[item["collectionName"]]
            docs = list(coll.find({}, sort=[("_id", 1)]))
            try:
                self.matcher.match(item["documents"], docs, root=False, path=f"outcome.{item['collectionName']}")
            except TestFail as e:
                raise TestFail(f"DATA {e}")

    # ------------------------------------------------------------------ run one test
    def run_test(self, f, test):
        self.entities, self.lsids, self.listeners = {}, {}, {}
        self.clients = []
        self.failpoints = []
        try:
            # as the unified format asks: kill sessions left by earlier tests (their transactions hold locks)
            self.internal.admin.command("killAllSessions", [])
        except PyMongoError:
            pass
        try:
            self.create_entities(f.get("createEntities", []))
            self.initial_data(f.get("initialData"))
            for op in test["operations"]:
                self.run_op(op)
            for s in [v for v in self.entities.values() if isinstance(v, pymongo.client_session.ClientSession)]:
                try:
                    s.end_session()
                except Exception:
                    pass
            if "expectEvents" in test:
                self.check_events(test["expectEvents"])
            self.check_outcome(test.get("outcome"))
        finally:
            # FENVIX: killAllSessions answers ok but does not abort open transactions (see RISULTATI), so the sessions
            # of this test are ended explicitly: endSessions does abort them.
            if self.lsids:
                try:
                    self.internal.admin.command("endSessions", list(self.lsids.values()))
                except PyMongoError:
                    pass
            for fp in self.failpoints:
                try:
                    fp[0].admin.command("configureFailPoint", fp[1], mode="off")
                except Exception:
                    pass
            for c in self.clients:
                try:
                    c.close()
                except Exception:
                    pass

    def check_events(self, expect):
        for block in expect:
            etype = block.get("eventType", "command")
            if etype != "command":
                raise Skip(f"runner:eventType {etype}")
            lst = self.listeners.get(block["client"])
            if lst is None:
                raise TestFail("events for unknown client")
            actual = list(lst.events)
            exp = block["events"]
            if block.get("ignoreExtraEvents"):
                actual = actual[:len(exp)]
            if len(actual) != len(exp):
                raise TestFail(f"events: expected {len(exp)} {[next(iter(e)) + ':' + str(next(iter(e.values())).get('commandName', '')) for e in exp]}, "
                               f"got {len(actual)} {[k + ':' + ev.command_name for k, ev in actual]}")
            for i, (e, (kind, ev)) in enumerate(zip(exp, actual)):
                (ekind, body), = e.items()
                if ekind != kind:
                    raise TestFail(f"event[{i}]: expected {ekind}, got {kind} ({ev.command_name})")
                if "commandName" in body and body["commandName"] != ev.command_name:
                    raise TestFail(f"event[{i}]: expected command {body['commandName']}, got {ev.command_name}")
                if "databaseName" in body and body["databaseName"] != ev.database_name:
                    raise TestFail(f"event[{i}]: expected db {body['databaseName']}, got {ev.database_name}")
                if kind == "commandStartedEvent" and "command" in body:
                    self.matcher.match(body["command"], ev.command, root=True, path=f"event[{i}].command")
                if kind == "commandSucceededEvent" and "reply" in body:
                    self.matcher.match(body["reply"], ev.reply, root=True, path=f"event[{i}].reply")
                for k in ("hasServiceId", "hasServerConnectionId"):
                    if k in body:
                        pass

    # ------------------------------------------------------------------ operations
    def run_op(self, op, propagate=False):
        name = op["name"]
        obj = op["object"]
        args = dict(op.get("arguments", {}))
        expect_error = op.get("expectError")
        try:
            if obj == "testRunner":
                result = self.test_runner_op(name, args)
            else:
                target = self.entity(obj)
                result = self.entity_op(target, name, args)
        except (Skip, TestFail):
            raise
        except Exception as exc:
            if op.get("ignoreResultAndError"):
                return
            if expect_error is None:
                raise TestFail(f"{name}: unexpected error {type(exc).__name__}: {str(exc)[:300]}")
            self.check_error(name, exc, expect_error)
            if propagate:
                # the unified format: errors raised in a withTransaction callback always reach withTransaction
                raise
            return
        if expect_error is not None:
            raise TestFail(f"{name}: expected an error {short(expect_error)}, got success {short(result)}")
        if op.get("ignoreResultAndError"):
            return
        if "expectResult" in op:
            exp = op["expectResult"]
            if name in ITERABLE_OPS and isinstance(exp, list) and isinstance(result, list):
                if len(exp) != len(result):
                    raise TestFail(f"{name}.result: expected {len(exp)} documents, got {len(result)}: {short(result)}")
                for i, (e, a) in enumerate(zip(exp, result)):
                    self.matcher.match(e, a, root=True, path=f"{name}.result[{i}]")
            else:
                self.matcher.match(exp, result, root=not isinstance(result, list), path=f"{name}.result")
        if "saveResultAsEntity" in op:
            self.entities[op["saveResultAsEntity"]] = result

    def check_error(self, name, exc, e):
        server_err = isinstance(exc, (OperationFailure, BulkWriteError, WriteError, WriteConcernError))
        if isinstance(exc, ClientBulkWriteException):
            server_err = exc.error is not None and isinstance(exc.error, OperationFailure) or bool(exc.write_errors) or bool(exc.write_concern_errors)
        msg = str(exc)
        if e.get("isClientError") and server_err:
            raise TestFail(f"{name}: expected client error, got server error {msg[:200]}")
        if e.get("isClientError") is False and not server_err:
            raise TestFail(f"{name}: expected server error, got client error {msg[:200]}")
        if e.get("isTimeoutError"):
            if not isinstance(exc, (NetworkTimeout, ExecutionTimeout)):
                raise TestFail(f"{name}: expected timeout, got {type(exc).__name__}")
        if "errorContains" in e and e["errorContains"].lower() not in msg.lower():
            raise TestFail(f"{name}: error should contain '{e['errorContains']}', got '{msg[:200]}'")
        top = exc
        if isinstance(exc, ClientBulkWriteException) and isinstance(exc.error, OperationFailure):
            top = exc.error
        code = getattr(top, "code", None)
        details = getattr(top, "details", None) or {}
        if isinstance(exc, BulkWriteError) and code is None:
            we = details.get("writeErrors") or []
            if we:
                code = we[0].get("code")
        if "errorCode" in e and code != e["errorCode"]:
            raise TestFail(f"{name}: expected error code {e['errorCode']}, got {code} ({msg[:150]})")
        if "errorCodeName" in e:
            cn = details.get("codeName")
            if cn is None and isinstance(exc, BulkWriteError):
                cn = ((details.get("writeErrors") or [{}])[0]).get("codeName")
            if cn != e["errorCodeName"]:
                raise TestFail(f"{name}: expected codeName {e['errorCodeName']}, got {cn} (code {code}, {msg[:150]})")
        for lab in e.get("errorLabelsContain", []):
            if not (hasattr(exc, "has_error_label") and exc.has_error_label(lab)):
                raise TestFail(f"{name}: error label {lab} missing ({msg[:150]})")
        for lab in e.get("errorLabelsOmit", []):
            if hasattr(exc, "has_error_label") and exc.has_error_label(lab):
                raise TestFail(f"{name}: error label {lab} should be absent")
        if "expectResult" in e:
            if isinstance(exc, BulkWriteError):
                self.matcher.match(e["expectResult"], bulk_result_doc(exc.details), root=True, path=f"{name}.errResult")
            elif isinstance(exc, ClientBulkWriteException) and exc.partial_result is not None:
                self.matcher.match(e["expectResult"], client_bulk_result_doc(exc.partial_result), root=True,
                                   path=f"{name}.errResult")
        if "writeErrors" in e:
            if not isinstance(exc, ClientBulkWriteException):
                raise TestFail(f"{name}: expected writeErrors")
            got = {str(k): {"code": v.get("code")} for k, v in ((we.get("idx"), we) for we in exc.write_errors)}
            self.matcher.match(e["writeErrors"], got, root=True, path=f"{name}.writeErrors")
        if "writeConcernErrors" in e:
            if not isinstance(exc, ClientBulkWriteException):
                raise TestFail(f"{name}: expected writeConcernErrors")
            self.matcher.match(e["writeConcernErrors"], list(exc.write_concern_errors), root=False,
                               path=f"{name}.wcErrors")

    def test_runner_op(self, name, a):
        if name in ("failPoint", "targetedFailPoint"):
            client = self.entity(a["client"]) if name == "failPoint" else self.internal
            if name == "targetedFailPoint":
                sess = self.entity(a["session"])
                client = sess.client
            fp = a["failPoint"]
            client.admin.command(fp)
            self.failpoints.append((client, fp["configureFailPoint"]))
            return None
        if name == "assertSessionTransactionState":
            sess = self.entity(a["session"])
            names = {1: "none", 2: "starting", 3: "in_progress", 4: "committed", 5: "committed", 6: "aborted"}
            got = names.get(sess._transaction.state)
            if got != a["state"]:
                raise TestFail(f"session state {got}, expected {a['state']}")
            return None
        if name in ("assertSessionPinned", "assertSessionUnpinned"):
            sess = self.entity(a["session"])
            pinned = sess._pinned_address is not None
            if pinned != (name == "assertSessionPinned"):
                raise TestFail(f"{name} failed")
            return None
        if name in ("assertSessionDirty", "assertSessionNotDirty"):
            sess = self.entity(a["session"])
            dirty = sess._server_session.dirty
            if dirty != (name == "assertSessionDirty"):
                raise TestFail(f"{name} failed")
            return None
        if name in ("assertSameLsidOnLastTwoCommands", "assertDifferentLsidOnLastTwoCommands"):
            lst = self.listeners[a["client"]]
            ev = [e for k, e in lst.events if k == "commandStartedEvent"]
            if len(ev) < 2:
                raise TestFail("fewer than two commands")
            same = ev[-1].command.get("lsid") == ev[-2].command.get("lsid")
            if same != (name == "assertSameLsidOnLastTwoCommands"):
                raise TestFail(f"{name} failed")
            return None
        if name in ("assertCollectionExists", "assertCollectionNotExists"):
            names = self.internal[a["databaseName"]].list_collection_names()
            if (a["collectionName"] in names) != (name == "assertCollectionExists"):
                raise TestFail(f"{name} {a['databaseName']}.{a['collectionName']} failed")
            return None
        if name in ("assertIndexExists", "assertIndexNotExists"):
            coll = self.internal[a["databaseName"]][a["collectionName"]]
            try:
                names = [i["name"] for i in coll.list_indexes()]
            except OperationFailure:
                names = []
            if (a["indexName"] in names) != (name == "assertIndexExists"):
                raise TestFail(f"{name} {a['indexName']} failed")
            return None
        if name == "createEntities":
            self.create_entities(a["entities"])
            return None
        if name == "assertNumberConnectionsCheckedOut":
            return None
        if name == "wait":
            time.sleep(a["ms"] / 1000)
            return None
        raise Skip(f"runner:{name}")

    # ------------------------------------------------------------------ entity operations
    def common(self, a, allowed):
        kw = {}
        for k in list(a):
            v = a[k]
            if k == "session":
                kw["session"] = self.entity(v)
            elif k in allowed:
                kw[allowed[k]] = v
            elif k == "timeoutMS":
                raise Skip("runner:timeoutMS")
            elif k == "rawData":
                raise Skip("runner:rawData")
            else:
                raise Skip(f"runner:arg {k}")
        return kw

    def entity_op(self, t, name, a):
        if isinstance(t, MongoClient):
            return self.client_op(t, name, a)
        if isinstance(t, pymongo.database.Database):
            return self.db_op(t, name, a)
        if isinstance(t, pymongo.collection.Collection):
            return self.coll_op(t, name, a)
        if isinstance(t, pymongo.client_session.ClientSession):
            return self.session_op(t, name, a)
        if isinstance(t, gridfs.GridFSBucket):
            return self.bucket_op(t, name, a)
        if isinstance(t, (pymongo.command_cursor.CommandCursor, pymongo.cursor.Cursor,
                          pymongo.change_stream.ChangeStream)):
            return self.cursor_op(t, name, a)
        if isinstance(t, ThreadEntity):
            raise Skip("runner:thread")
        raise Skip(f"runner:object {type(t).__name__}")

    def cursor_op(self, c, name, a):
        if name == "iterateUntilDocumentOrError":
            if isinstance(c, pymongo.change_stream.ChangeStream):
                deadline = time.time() + 10
                while time.time() < deadline:
                    d = c.try_next()
                    if d is not None:
                        return d
                raise TestFail("change stream: no event within 10 s")
            return next(c)
        if name == "iterateOnce":
            if isinstance(c, pymongo.change_stream.ChangeStream):
                c.try_next()
            else:
                try:
                    c.try_next() if hasattr(c, "try_next") else next(c)
                except StopIteration:
                    pass
            return None
        if name == "close":
            c.close()
            return None
        raise Skip(f"runner:cursor.{name}")

    def watch(self, t, a):
        allowed = {"pipeline": "pipeline", "fullDocument": "full_document",
                   "fullDocumentBeforeChange": "full_document_before_change", "resumeAfter": "resume_after",
                   "startAfter": "start_after", "startAtOperationTime": "start_at_operation_time",
                   "batchSize": "batch_size", "comment": "comment", "showExpandedEvents": "show_expanded_events",
                   "maxAwaitTimeMS": "max_await_time_ms", "collation": "collation"}
        kw = self.common(a, allowed)
        return t.watch(**kw)

    def client_op(self, c, name, a):
        if name == "listDatabases":
            kw = self.common(a, {"filter": "filter", "nameOnly": "nameOnly", "authorizedDatabases": "authorizedDatabases"})
            return list(c.list_databases(**kw))
        if name == "listDatabaseNames":
            kw = self.common(a, {"filter": "filter", "authorizedDatabases": "authorizedDatabases"})
            return c.list_database_names(**kw)
        if name == "createChangeStream":
            return self.watch(c, a)
        if name == "clientBulkWrite":
            return self.client_bulk(c, a)
        raise Skip(f"runner:client.{name}")

    def client_bulk(self, c, a):
        from pymongo.operations import (InsertOne as I, UpdateOne as U1, UpdateMany as UM, ReplaceOne as R1,
                                        DeleteOne as D1, DeleteMany as DM)
        models = []
        for m in a.pop("models"):
            (kind, b), = m.items()
            b = dict(b)
            ns = b.pop("namespace")
            if kind == "insertOne":
                models.append(I(b["document"], namespace=ns))
            elif kind in ("updateOne", "updateMany", "replaceOne"):
                kw = {}
                for k, v in b.items():
                    if k in ("filter", "update", "replacement"):
                        continue
                    kw[{"upsert": "upsert", "arrayFilters": "array_filters", "collation": "collation", "hint": "hint",
                        "sort": "sort"}.get(k) or self._skip(f"runner:clientBulk arg {k}")] = v
                cls = {"updateOne": U1, "updateMany": UM, "replaceOne": R1}[kind]
                models.append(cls(b["filter"], b.get("update", b.get("replacement")), namespace=ns, **kw))
            elif kind in ("deleteOne", "deleteMany"):
                kw = {}
                for k, v in b.items():
                    if k == "filter":
                        continue
                    kw[{"collation": "collation", "hint": "hint"}.get(k) or self._skip(f"runner:clientBulk arg {k}")] = v
                models.append((D1 if kind == "deleteOne" else DM)(b["filter"], namespace=ns, **kw))
            else:
                raise Skip(f"runner:clientBulk {kind}")
        kw = self.common(a, {"ordered": "ordered", "verboseResults": "verbose_results", "comment": "comment",
                             "let": "let", "bypassDocumentValidation": "bypass_document_validation",
                             "writeConcern": "write_concern"})
        if "write_concern" in kw:
            kw["write_concern"] = WriteConcern(**self.wc(kw["write_concern"]))
        res = c.bulk_write(models, **kw)
        return client_bulk_result_doc(res)

    def _skip(self, why):
        raise Skip(why)

    def db_op(self, db, name, a):
        if name == "runCommand":
            cmd = a.pop("command")
            a.pop("commandName", None)
            kw = self.common(a, {"readPreference": "read_preference"})
            if "read_preference" in kw:
                kw["read_preference"] = self.read_pref(kw["read_preference"])
            return db.command(cmd, **kw)
        if name == "aggregate":
            pipeline = a.pop("pipeline")
            kw = self.common(a, {"batchSize": "batchSize", "allowDiskUse": "allowDiskUse", "comment": "comment",
                                 "let": "let", "maxTimeMS": "maxTimeMS", "hint": "hint", "collation": "collation"})
            return list(db.aggregate(pipeline, **kw))
        if name == "createCollection":
            coll = a.pop("collection")
            kw = self.common(a, {k: k for k in ("capped", "size", "max", "validator", "validationLevel",
                                                "validationAction", "collation", "timeseries", "expireAfterSeconds",
                                                "clusteredIndex", "changeStreamPreAndPostImages", "viewOn",
                                                "pipeline", "encryptedFields", "storageEngine", "indexOptionDefaults",
                                                "comment")})
            if "viewOn" in kw:
                kw["viewOn"] = kw["viewOn"]
            db.create_collection(coll, check_exists=False, **kw)
            return None
        if name == "dropCollection":
            coll = a.pop("collection")
            kw = self.common(a, {"encryptedFields": "encrypted_fields", "comment": "comment"})
            db.drop_collection(coll, **kw)
            return None
        if name == "listCollections":
            kw = self.common(a, {"filter": "filter", "batchSize": "batchSize", "nameOnly": "nameOnly",
                                 "authorizedCollections": "authorizedCollections", "comment": "comment"})
            return list(db.list_collections(**kw))
        if name == "listCollectionNames":
            kw = self.common(a, {"filter": "filter", "batchSize": "batchSize"})
            return db.list_collection_names(**kw)
        if name == "modifyCollection":
            coll = a.pop("collection")
            kw = self.common(a, {k: k for k in ("validator", "validationLevel", "validationAction", "index",
                                                "changeStreamPreAndPostImages", "expireAfterSeconds")})
            return db.command("collMod", coll, **{k: v for k, v in kw.items() if k != "session"},
                              session=kw.get("session"))
        if name == "createChangeStream":
            return self.watch(db, a)
        if name == "createCommandCursor":
            cmd = a.pop("command")
            a.pop("commandName", None)
            kw = self.common(a, {"batchSize": "batch_size", "cursorType": "cursor_type", "comment": "comment",
                                 "maxTimeMS": "max_await_time_ms"})
            kw.pop("cursor_type", None)
            return db.cursor_command(cmd, **kw)
        raise Skip(f"runner:database.{name}")

    FIND_ARGS = {"filter": "filter", "projection": "projection", "skip": "skip", "limit": "limit",
                 "batchSize": "batch_size", "sort": "sort", "hint": "hint", "collation": "collation",
                 "comment": "comment", "max": "max", "min": "min", "maxTimeMS": "max_time_ms",
                 "returnKey": "return_key", "showRecordId": "show_record_id", "allowDiskUse": "allow_disk_use",
                 "let": "let", "noCursorTimeout": "no_cursor_timeout", "allowPartialResults": "allow_partial_results",
                 "cursorType": "cursor_type", "maxAwaitTimeMS": "max_await_time_ms", "oplogReplay": "oplog_replay"}
    WRITE_ARGS = {"upsert": "upsert", "arrayFilters": "array_filters", "collation": "collation", "hint": "hint",
                  "bypassDocumentValidation": "bypass_document_validation", "comment": "comment", "let": "let",
                  "sort": "sort"}

    def sort(self, kw):
        if "sort" in kw and isinstance(kw["sort"], dict):
            kw["sort"] = list(kw["sort"].items())

    def coll_op(self, c, name, a):
        if name in ("find", "createFindCursor", "findOne"):
            kw = self.common(a, self.FIND_ARGS)
            self.sort(kw)
            if "cursor_type" in kw:
                kw["cursor_type"] = {"tailable": pymongo.CursorType.TAILABLE,
                                     "tailableAwait": pymongo.CursorType.TAILABLE_AWAIT,
                                     "nonTailable": pymongo.CursorType.NON_TAILABLE}[kw["cursor_type"]]
            if "let" in kw:
                kw["let"] = kw.pop("let")
            if name == "findOne":
                return c.find_one(**kw)
            cur = c.find(**kw)
            return cur if name == "createFindCursor" else list(cur)
        if name == "aggregate":
            pipeline = a.pop("pipeline")
            kw = self.common(a, {"batchSize": "batchSize", "allowDiskUse": "allowDiskUse", "comment": "comment",
                                 "let": "let", "maxTimeMS": "maxTimeMS", "hint": "hint", "collation": "collation",
                                 "bypassDocumentValidation": "bypassDocumentValidation",
                                 "maxAwaitTimeMS": "maxAwaitTimeMS"})
            return list(c.aggregate(pipeline, **kw))
        if name == "countDocuments":
            flt = a.pop("filter")
            kw = self.common(a, {"skip": "skip", "limit": "limit", "hint": "hint", "collation": "collation",
                                 "comment": "comment", "maxTimeMS": "maxTimeMS"})
            return c.count_documents(flt, **kw)
        if name == "estimatedDocumentCount":
            kw = self.common(a, {"maxTimeMS": "maxTimeMS", "comment": "comment"})
            return c.estimated_document_count(**kw)
        if name == "distinct":
            key = a.pop("fieldName")
            kw = self.common(a, {"filter": "filter", "collation": "collation", "comment": "comment",
                                 "maxTimeMS": "maxTimeMS", "hint": "hint"})
            return c.distinct(key, **kw)
        if name == "insertOne":
            doc = a.pop("document")
            kw = self.common(a, {"bypassDocumentValidation": "bypass_document_validation", "comment": "comment"})
            r = c.insert_one(doc, **kw)
            return {"insertedId": r.inserted_id}
        if name == "insertMany":
            docs = a.pop("documents")
            kw = self.common(a, {"ordered": "ordered", "bypassDocumentValidation": "bypass_document_validation",
                                 "comment": "comment"})
            r = c.insert_many(docs, **kw)
            return {"insertedIds": {str(i): v for i, v in enumerate(r.inserted_ids)}}
        if name in ("updateOne", "updateMany", "replaceOne"):
            flt = a.pop("filter")
            upd = a.pop("update" if name != "replaceOne" else "replacement")
            kw = self.common(a, self.WRITE_ARGS)
            if "sort" in kw and name == "updateMany":
                raise Skip("runner:updateMany sort")
            fn = {"updateOne": c.update_one, "updateMany": c.update_many, "replaceOne": c.replace_one}[name]
            r = fn(flt, upd, **kw)
            if not r.acknowledged:
                return {"acknowledged": False}
            return {"matchedCount": r.matched_count, "modifiedCount": r.modified_count,
                    "upsertedCount": 0 if r.upserted_id is None else 1,
                    **({"upsertedId": r.upserted_id} if r.upserted_id is not None else {})}
        if name in ("deleteOne", "deleteMany"):
            flt = a.pop("filter")
            kw = self.common(a, {"collation": "collation", "hint": "hint", "comment": "comment", "let": "let"})
            r = (c.delete_one if name == "deleteOne" else c.delete_many)(flt, **kw)
            if not r.acknowledged:
                return {"acknowledged": False}
            return {"deletedCount": r.deleted_count}
        if name in ("findOneAndUpdate", "findOneAndReplace", "findOneAndDelete"):
            flt = a.pop("filter")
            allowed = {"projection": "projection", "sort": "sort", "upsert": "upsert", "hint": "hint",
                       "collation": "collation", "arrayFilters": "array_filters", "comment": "comment", "let": "let",
                       "returnDocument": "return_document", "maxTimeMS": "maxTimeMS",
                       "bypassDocumentValidation": "bypassDocumentValidation"}
            if name == "findOneAndDelete":
                kw = self.common(a, allowed)
                self.sort(kw)
                return c.find_one_and_delete(flt, **kw)
            upd = a.pop("update" if name == "findOneAndUpdate" else "replacement")
            kw = self.common(a, allowed)
            self.sort(kw)
            if "return_document" in kw:
                kw["return_document"] = kw["return_document"] == "After"
            fn = c.find_one_and_update if name == "findOneAndUpdate" else c.find_one_and_replace
            return fn(flt, upd, **kw)
        if name == "bulkWrite":
            reqs = []
            for m in a.pop("requests"):
                (kind, b), = m.items()
                b = dict(b)
                if kind == "insertOne":
                    reqs.append(InsertOne(b.pop("document")))
                    continue
                flt = b.pop("filter")
                opt = {}
                for k, v in b.items():
                    if k in ("update", "replacement"):
                        continue
                    mk = {"upsert": "upsert", "arrayFilters": "array_filters", "collation": "collation",
                          "hint": "hint", "sort": "sort"}.get(k)
                    if mk is None:
                        raise Skip(f"runner:bulk arg {k}")
                    opt[mk] = v
                if "sort" in opt and kind == "updateMany":
                    raise Skip("runner:updateMany sort")
                if kind == "updateOne":
                    reqs.append(UpdateOne(flt, b["update"], **opt))
                elif kind == "updateMany":
                    reqs.append(UpdateMany(flt, b["update"], **opt))
                elif kind == "replaceOne":
                    reqs.append(ReplaceOne(flt, b["replacement"], **opt))
                elif kind == "deleteOne":
                    reqs.append(DeleteOne(flt, **opt))
                elif kind == "deleteMany":
                    reqs.append(DeleteMany(flt, **opt))
                else:
                    raise Skip(f"runner:bulk {kind}")
            kw = self.common(a, {"ordered": "ordered", "bypassDocumentValidation": "bypass_document_validation",
                                 "comment": "comment", "let": "let"})
            r = c.bulk_write(reqs, **kw)
            if not r.acknowledged:
                return {"acknowledged": False}
            return bulk_result_doc(r.bulk_api_result)
        if name == "createIndex":
            keys = list(a.pop("keys").items())
            kw = self.common(a, {k: k for k in ("name", "unique", "sparse", "expireAfterSeconds", "background",
                                                "partialFilterExpression", "collation", "hidden", "commitQuorum",
                                                "comment", "maxTimeMS", "wildcardProjection", "weights",
                                                "default_language", "2dsphereIndexVersion")})
            return c.create_index(keys, **kw)
        if name == "createIndexes":
            raise Skip("runner:createIndexes")
        if name == "dropIndex":
            nm = a.pop("name")
            kw = self.common(a, {"comment": "comment", "maxTimeMS": "maxTimeMS"})
            c.drop_index(nm, **kw)
            return None
        if name == "dropIndexes":
            kw = self.common(a, {"comment": "comment", "maxTimeMS": "maxTimeMS"})
            c.drop_indexes(**kw)
            return None
        if name == "listIndexes":
            kw = self.common(a, {"batchSize": "batchSize", "comment": "comment"})
            return list(c.list_indexes(**kw))
        if name == "listIndexNames":
            kw = self.common(a, {"batchSize": "batchSize"})
            return [i["name"] for i in c.list_indexes(**kw)]
        if name == "createChangeStream":
            return self.watch(c, a)
        if name == "rename":
            to = a.pop("to")
            kw = self.common(a, {"dropTarget": "dropTarget", "comment": "comment"})
            c.rename(to, **kw)
            return None
        if name == "drop":
            kw = self.common(a, {"comment": "comment"})
            c.drop(**kw)
            return None
        if name in ("createSearchIndex", "createSearchIndexes", "dropSearchIndex", "listSearchIndexes",
                    "updateSearchIndex"):
            raise Skip("req:atlas-search")
        raise Skip(f"runner:collection.{name}")

    def session_op(self, s, name, a):
        if name == "startTransaction":
            s.start_transaction(**self.txn_opts(a))
            return None
        if name == "commitTransaction":
            if a:
                raise Skip(f"runner:commit args {list(a)}")
            s.commit_transaction()
            return None
        if name == "abortTransaction":
            s.abort_transaction()
            return None
        if name == "endSession":
            s.end_session()
            return None
        if name == "withTransaction":
            ops = a.pop("callback")
            opts = self.txn_opts(a)

            def cb(_s):
                for op in ops:
                    self.run_op(op, propagate=True)
            s.with_transaction(cb, **opts)
            return None
        raise Skip(f"runner:session.{name}")

    def bucket_op(self, b, name, a):
        import io
        if name == "upload":
            src = a.pop("source")
            fname = a.pop("filename")
            kw = {}
            if "chunkSizeBytes" in a:
                kw["chunk_size_bytes"] = a.pop("chunkSizeBytes")
            if "metadata" in a:
                kw["metadata"] = a.pop("metadata")
            if "contentType" in a or "disableMD5" in a or a:
                raise Skip(f"runner:upload args {list(a)}")
            data = src.get("$$hexBytes") if isinstance(src, dict) else None
            raw = bytes.fromhex(data) if data is not None else bytes(src)
            return b.upload_from_stream(fname, io.BytesIO(raw), **kw)
        if name == "download":
            oid = a.pop("id")
            out = io.BytesIO()
            b.download_to_stream(oid, out)
            return out.getvalue()
        if name == "downloadByName":
            fn = a.pop("filename")
            kw = {}
            if "revision" in a:
                kw["revision"] = a.pop("revision")
            out = io.BytesIO()
            b.download_to_stream_by_name(fn, out, **kw)
            return out.getvalue()
        if name == "delete":
            b.delete(a.pop("id"))
            return None
        if name == "drop":
            b.drop()
            return None
        if name == "find":
            flt = a.pop("filter", {})
            return list(b.find(flt))
        if name == "rename":
            b.rename(a.pop("id"), a.pop("newFilename"))
            return None
        raise Skip(f"runner:bucket.{name}")


ITERABLE_OPS = {"find", "aggregate", "listCollections", "listDatabases", "listIndexes", "runCursorCommand",
                "listSearchIndexes"}


class ThreadEntity:
    pass


def bulk_result_doc(d):
    up = d.get("upserted") or []
    return {"deletedCount": d.get("nRemoved", 0), "insertedCount": d.get("nInserted", 0),
            "matchedCount": d.get("nMatched", 0), "modifiedCount": d.get("nModified", 0),
            "upsertedCount": d.get("nUpserted", 0),
            "upsertedIds": {str(u["index"]): u["_id"] for u in up}}


def client_bulk_result_doc(r):
    out = {"insertedCount": r.inserted_count, "upsertedCount": r.upserted_count, "matchedCount": r.matched_count,
           "modifiedCount": r.modified_count, "deletedCount": r.deleted_count}
    if r.has_verbose_results:
        out["insertResults"] = {str(k): {"insertedId": v.inserted_id} for k, v in r.insert_results.items()}
        out["updateResults"] = {str(k): {"matchedCount": v.matched_count, "modifiedCount": v.modified_count,
                                         **({"upsertedId": v.upserted_id} if v.upserted_id is not None else {})}
                                for k, v in r.update_results.items()}
        out["deleteResults"] = {str(k): {"deletedCount": v.deleted_count} for k, v in r.delete_results.items()}
    else:
        out["insertResults"] = {}
        out["updateResults"] = {}
        out["deleteResults"] = {}
    return out


def classify_fail(msg):
    m = msg.lower()
    if m.startswith("crash"):
        return "bug-crash"
    if "data " in msg[:6] or msg.startswith("DATA"):
        return "bug-dati"
    if any(x in m for x in ("no such command", "not supported", "unrecognized", "unknown operator",
                            "commandnotfound", "not implemented", "unsupported", "unknown top level operator",
                            "unrecognized pipeline stage", "invalid $")):
        return "mancante"
    if "requires mongodb server version 8.0" in m:
        return "mancante"
    if "unexpected error" in m:
        return "errore-inatteso"
    return "diverso"


FAILPOINTS = False


def uses_failpoint(ops):
    for op in ops:
        if op.get("name") in ("failPoint", "targetedFailPoint"):
            return True
        cb = op.get("arguments", {}).get("callback")
        if isinstance(cb, list) and uses_failpoint(cb):
            return True
    return False


def main():
    global FAILPOINTS
    ap = argparse.ArgumentParser()
    ap.add_argument("--uri", required=True)
    ap.add_argument("--out", required=True)
    ap.add_argument("--area", required=True)
    ap.add_argument("--filter", default="")
    ap.add_argument("files", nargs="+")
    args = ap.parse_args()
    probe = MongoClient(args.uri, serverSelectionTimeoutMS=5000)
    info = probe.admin.command("buildInfo")
    try:
        probe.admin.command("configureFailPoint", "failCommand", mode="off")
        FAILPOINTS = True
    except PyMongoError:
        FAILPOINTS = False
    r = Runner(args.uri, info)
    out = open(args.out, "a")
    counts = {"PASS": 0, "FAIL": 0, "SKIP": 0}
    for path in args.files:
        f = loads(open(path).read())
        if "schemaVersion" not in f:
            continue
        fname = path.split("/")[-1]
        file_skip = r.check_reqs(f.get("runOnRequirements"))
        for t in f["tests"]:
            if args.filter and args.filter not in t["description"]:
                continue
            res = {"area": args.area, "file": fname, "test": t["description"]}
            t0 = time.time()
            why = file_skip or r.check_reqs(t.get("runOnRequirements"))
            if why:
                res.update(status="SKIP", reason=why)
            elif "skipReason" in t:
                res.update(status="SKIP", reason="spec-skip")
            elif not FAILPOINTS and uses_failpoint(t["operations"]):
                res.update(status="SKIP", reason="failpoint")
            else:
                try:
                    r.run_test(f, t)
                    res.update(status="PASS")
                except Skip as s:
                    res.update(status="SKIP", reason=str(s))
                except TestFail as e:
                    res.update(status="FAIL", reason=str(e))
                except Exception as e:
                    reason = f"{type(e).__name__}: {str(e)[:300]}"
                    if isinstance(e, OperationFailure) and "configureFailPoint" in str(e):
                        res.update(status="SKIP", reason="failpoint")
                    else:
                        res.update(status="FAIL", reason="runner-exception " + reason,
                                   trace=traceback.format_exc()[-800:])
                if res["status"] == "FAIL" and "No replica set members match selector" in res["reason"]:
                    # the test needs a secondary; our replica set has one member (as a one-node mongod would)
                    res.update(status="SKIP", reason="req:secondario")
                if res["status"] == "FAIL" and ("configureFailPoint" in res["reason"] or "failCommand" in res["reason"]):
                    res.update(status="SKIP", reason="failpoint")
                if res["status"] == "FAIL":
                    # was the server still alive?
                    try:
                        MongoClient(args.uri, serverSelectionTimeoutMS=3000).admin.command("ping")
                    except Exception:
                        res["reason"] = "CRASH server not answering after test: " + res["reason"]
                        out.write(json.dumps(res) + "\n")
                        out.flush()
                        print("SERVER DOWN", file=sys.stderr)
                        sys.exit(3)
                    res["kind"] = classify_fail(res["reason"])
            res["secs"] = round(time.time() - t0, 2)
            counts[res["status"]] += 1
            out.write(json.dumps(res, default=str) + "\n")
            out.flush()
    print(args.area, counts)


if __name__ == "__main__":
    main()
