#!/usr/bin/env node
// Differential test runner (official Node.js "mongodb" driver). Same scripts, same comparison
// rules as run_py.py: ObjectIds are normalised unless the test supplied them, top-level result
// lists are sorted when the test says the order is unspecified, everything else is compared exactly
// as canonical Extended JSON. Replies are deserialised with promoteValues:false so int32, int64
// and double stay distinct.
'use strict';
const fs = require('fs');
const path = require('path');
const { execFileSync } = require('child_process');
const net = require('net');
const { MongoClient, ObjectId, Long, BSON } = require('mongodb');
const { EJSON } = BSON;

const HERE = path.resolve(__dirname, '..');
const SKIP = Symbol('skip');
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

function generatedDocs(kind, n) {
  const tags = ['x', 'y', 'z'];
  return Array.from({ length: n }, (_, i) => ({ _id: i, n: i, grp: i % 10, name: `user${i}`, tag: tags[i % 3], nested: { k: i % 7 } }));
}
const swapDb = (v, db) => {
  if (typeof v === 'string') return v.split('$DB').join(db);
  if (Array.isArray(v)) return v.map((x) => swapDb(x, db));
  if (v && typeof v === 'object' && !v._bsontype && !(v instanceof Date)) return Object.fromEntries(Object.entries(v).map(([k, x]) => [k, swapDb(x, db)]));
  return v;
};
// Undo gen_tests' escaping of the legacy `$regex`/`$options` keys (the EJSON parser would turn them into regex values).
const unescape = (v) => {
  if (Array.isArray(v)) return v.map(unescape);
  if (v && typeof v === 'object' && !v._bsontype && !(v instanceof Date) && !Buffer.isBuffer(v)) {
    return Object.fromEntries(Object.entries(v).map(([k, x]) => [k === '@@regex' ? '$regex' : k === '@@options' ? '$options' : k, unescape(x)]));
  }
  return v;
};
const clone = (v) => EJSON.parse(EJSON.stringify(v, { relaxed: false }), { relaxed: false });
const num = (v) => (v && typeof v.valueOf === 'function' ? Number(v.valueOf()) : v);

class Ctx {
  constructor(uri, port, idleSecs, isOurs) {
    Object.assign(this, { uri, port, idleSecs, isOurs, asserts: [] });
    this.client = this.newClient();
  }
  newClient() {
    return new MongoClient(this.uri, { serverSelectionTimeoutMS: 15000, socketTimeoutMS: 60000, promoteValues: false });
  }
  async connect() { await this.client.connect(); }
  async reconnect() { await this.client.close(); this.client = this.newClient(); await this.client.connect(); }
}

const val = (x) => (x && x.valueOf && x._bsontype ? x.valueOf() : x);
function errResult(e, cmpMsg) {
  const name = e && e.constructor && e.constructor.name;
  if (name === 'MongoBulkWriteError') {
    const we = (Array.isArray(e.writeErrors) ? e.writeErrors : e.writeErrors ? [e.writeErrors] : []).map((w) => {
      const x = { index: w.index, code: w.code };
      if (cmpMsg) { x.errmsg = w.errmsg; }
      return x;
    });
    const r = e.result || {};
    return { __bulk_error__: { nInserted: num(r.insertedCount), nMatched: num(r.matchedCount), nModified: num(r.modifiedCount), nRemoved: num(r.deletedCount), nUpserted: num(r.upsertedCount), writeErrors: we } };
  }
  if (name === 'MongoServerError' || (e && e.code !== undefined && e.codeName !== undefined)) {
    const r = { code: num(e.code) };
    if (num(e.code) !== 11000) r.codeName = e.codeName;
    if (cmpMsg) { r.msg = e.errmsg || e.message; if (num(e.code) === 11000) { r.keyPattern = e.keyPattern; r.keyValue = e.keyValue; } }
    return { __error__: r };
  }
  return { __exception__: name, text: String(e.message).slice(0, 200) };
}

function shape(plan) {
  if (!plan || typeof plan !== 'object') return null;
  const ins = plan.inputStages ?? (plan.inputStage ? [plan.inputStage] : []);
  const s = { stage: plan.stage };
  if (ins.length) s.in = ins.map(shape);
  if (plan.stage === 'IXSCAN') for (const k of ['indexName', 'direction', 'isMultiKey', 'isUnique', 'isSparse', 'isPartial', 'keyPattern', 'indexBounds']) if (k in plan) s[k] = plan[k];
  return s;
}
function reduceExplain(doc, stats) {
  const out = { plan: shape(doc.queryPlanner.winningPlan) };
  if (stats) { const es = doc.executionStats; out.stats = {}; for (const k of ['nReturned', 'totalKeysExamined', 'totalDocsExamined']) if (k in es) out.stats[k] = es[k]; }
  return out;
}
const pick = (doc, keys, reduce) => {
  if (reduce === 'collstats') return { nindexes: doc.nindexes, count: doc.count, indexNames: Object.keys(doc.indexSizes).sort(), ok: doc.ok };
  if (reduce === 'explain') return reduceExplain(doc, false);
  if (reduce === 'explain_stats') return reduceExplain(doc, true);
  if (!keys) { const o = { ...doc }; for (const k of ['operationTime', '$clusterTime', 'localTime', 'connectionId', 'topologyVersion', '$db']) delete o[k]; return o; }
  return Object.fromEntries(keys.filter((k) => k in doc).map((k) => [k, doc[k]]));
};

async function rawBatches(db, coll, first) {
  const sizes = [first.cursor.firstBatch.length];
  const ids = first.cursor.firstBatch.map((d) => num(d._id));
  let cid = first.cursor.id;
  while (!Long.fromValue(cid).isZero()) {
    const r = await db.command({ getMore: cid, collection: coll });
    sizes.push(r.cursor.nextBatch.length);
    ids.push(...r.cursor.nextBatch.map((d) => num(d._id)));
    cid = r.cursor.id;
  }
  return { batch_sizes: sizes, ids_sorted: ids.every((v, i) => i === 0 || ids[i - 1] <= v), n: ids.length };
}

// Legacy OP_QUERY `isMaster` to admin.$cmd on a plain socket; parse the OP_REPLY.
function rawOpQuery(ctx) {
  const [host, port] = ctx.uri.split('//')[1].split(':');
  const q = BSON.serialize({ ismaster: 1, helloOk: true });
  const ns = Buffer.from('admin.$cmd\0');
  const body = Buffer.concat([Buffer.alloc(4), ns, Buffer.from(new Int32Array([0, -1]).buffer), q]);
  const head = Buffer.alloc(16);
  head.writeInt32LE(16 + body.length, 0); head.writeInt32LE(4242, 4); head.writeInt32LE(0, 8); head.writeInt32LE(2004, 12);
  return new Promise((resolve, reject) => {
    const s = net.connect({ host, port: Number(port) }, () => s.write(Buffer.concat([head, body])));
    let buf = Buffer.alloc(0);
    s.on('data', (d) => {
      buf = Buffer.concat([buf, d]);
      if (buf.length >= 16 && buf.length >= buf.readInt32LE(0)) {
        s.end();
        const rest = buf.subarray(16);
        const doc = BSON.deserialize(rest.subarray(20), { promoteValues: false });
        const keep = ['ismaster', 'helloOk', 'maxBsonObjectSize', 'maxMessageSizeBytes', 'maxWriteBatchSize', 'minWireVersion', 'maxWireVersion', 'readOnly', 'ok'];
        resolve({ opcode: buf.readInt32LE(12), response_to: buf.readInt32LE(8), response_flags: rest.readInt32LE(0), cursor_id: Long.fromBigInt(rest.readBigInt64LE(4)), starting_from: rest.readInt32LE(12), number_returned: rest.readInt32LE(16), doc: Object.fromEntries(keep.filter((k) => k in doc).map((k) => [k, doc[k]])) });
      }
    });
    s.on('error', reject);
  });
}

function substNow(x) {
  if (Array.isArray(x)) return x.map(substNow);
  if (x && typeof x === 'object' && !(x instanceof Date) && x.constructor === Object) {
    const ks = Object.keys(x);
    if (ks.length === 1 && ks[0] === '$nowOffsetMs') return new Date(Math.floor(Date.now() / 1000) * 1000 + Number(x.$nowOffsetMs));
    return Object.fromEntries(ks.map((k) => [k, substNow(x[k])]));
  }
  return x;
}

const WATCH_OPT = { full_document: 'fullDocument', resume_after: 'resumeAfter', start_after: 'startAfter', show_expanded_events: 'showExpandedEvents' };
function stripEvent(e) { const o = { ...e }; for (const k of ['_id', 'clusterTime', 'wallTime']) delete o[k]; if ('txnNumber' in o) { o.txnNumber = '<n>'; o.lsid = '<lsid>'; } return o; }
async function collect(st, n) {
  const out = [];
  for (let i = 0; i < 8 && out.length < n; i++) { const e = await st.tryNext(); if (e) out.push(stripEvent(e)); }
  return out;
}
async function runWatch(ctx, db, dbname, step) {
  const target = step.cluster ? ctx.client : (step.coll ? db.collection(step.coll) : db);
  const opts = {}; for (const [k, v] of Object.entries(step.options ?? {})) opts[WATCH_OPT[k] ?? k] = v;
  const st = target.watch(step.pipeline ?? [], opts);
  for (const a of step.actions ?? []) await execStep(ctx, db, dbname, { ...a });
  const first = await collect(st, step.expect ?? 0);
  if (!step.resume_actions) { await st.close(); return first; }
  const token = st.resumeToken; await st.close();
  for (const a of step.resume_actions) await execStep(ctx, db, dbname, { ...a });
  const st2 = target.watch(step.pipeline ?? [], { ...opts, [WATCH_OPT[step.resume_with ?? 'resume_after']]: token });
  const second = await collect(st2, step.resume_expect ?? 0); await st2.close();
  return { first, resumed: second };
}

async function execStep(ctx, db, dbname, step) {
  step = substNow(step);
  if (step.op === 'watch') return await runWatch(ctx, db, dbname, step);
  ctx.sessions = ctx.sessions ?? {};
  const ses = step.session !== undefined ? ctx.sessions[step.session] : undefined;
  const so = ses ? { session: ses } : {};
  if (step.op === 'txn_start') { const s = ctx.client.startSession(); s.startTransaction(); ctx.sessions[step.name] = s; return SKIP; }
  if (step.op === 'txn_commit' || step.op === 'txn_abort') {
    const s = ctx.sessions[step.session]; delete ctx.sessions[step.session];
    try { await (step.op === 'txn_commit' ? s.commitTransaction() : s.abortTransaction()); } finally { await s.endSession(); }
    return 'ok';
  }
  if (step.op === 'with_txn') {
    const s = ctx.client.startSession(); const out = [];
    try {
      await s.withTransaction(async () => {
        out.length = 0; ctx.sessions.__wt = s;
        for (const st of step.steps) out.push(await execStep(ctx, db, dbname, { ...st, session: '__wt' }));
      });
    } finally { delete ctx.sessions.__wt; await s.endSession(); }
    return out;
  }
  const op = step.op;
  const coll = step.coll ? db.collection(step.coll) : null;
  switch (op) {
    case 'insert_one': return { inserted_id: (await coll.insertOne(step.doc, so)).insertedId };
    case 'insert_many': return { inserted_ids: Object.values((await coll.insertMany(step.docs, { ordered: step.ordered ?? true, ...so })).insertedIds) };
    case 'insert_generated': {
      const docs = generatedDocs(step.kind, num(step.n));
      for (let i = 0; i < docs.length; i += 500) await coll.insertMany(docs.slice(i, i + 500), { ordered: step.ordered ?? true });
      return { inserted: docs.length };
    }
    case 'find': case 'find_one': case 'find_count': {
      const o = { ...so };
      if (step.projection) o.projection = step.projection;
      if (step.sort) o.sort = step.sort;
      if (step.hint !== undefined) o.hint = step.hint;
      if (step.skip !== undefined) o.skip = num(step.skip);
      if (step.limit !== undefined) o.limit = num(step.limit);
      if (step.batch_size !== undefined) o.batchSize = num(step.batch_size);
      if (op === 'find_one') {
        delete o.limit;
        const d = await coll.findOne(step.filter ?? {}, o);
        return step.keys_only && d ? Object.keys(d) : d;
      }
      const docs = await coll.find(step.filter ?? {}, o).toArray();
      return op === 'find_count' ? docs.length : docs;
    }
    case 'count_documents': {
      const o = { ...so };
      if (step.skip !== undefined) o.skip = num(step.skip);
      if (step.limit !== undefined) o.limit = num(step.limit);
      return await coll.countDocuments(step.filter ?? {}, o);
    }
    case 'estimated_count': return await coll.estimatedDocumentCount();
    case 'aggregate': return await coll.aggregate(step.pipeline, step.batch_size !== undefined ? { batchSize: num(step.batch_size), ...so } : { ...so }).toArray();
    case 'distinct': return await coll.distinct(step.key, step.filter ?? {}, so);
    case 'update_one': case 'update_many': case 'replace_one': {
      const o = { upsert: step.upsert ?? false, ...so };
      if (step.array_filters) o.arrayFilters = step.array_filters;
      const fn = { update_one: 'updateOne', update_many: 'updateMany', replace_one: 'replaceOne' }[op];
      const r = await coll[fn](step.filter, step.update, o);
      return { matched: r.matchedCount, modified: r.modifiedCount, upserted_id: r.upsertedId ?? null };
    }
    case 'delete_one': return { deleted: (await coll.deleteOne(step.filter, so)).deletedCount };
    case 'delete_many': return { deleted: (await coll.deleteMany(step.filter, so)).deletedCount };
    case 'find_one_and_update': case 'find_one_and_replace': case 'find_one_and_delete': {
      const o = { ...so };
      if (step.projection) o.projection = step.projection;
      if (step.sort) o.sort = step.sort;
      if (op !== 'find_one_and_delete') {
        o.upsert = step.upsert ?? false;
        o.returnDocument = step.return_document === 'after' ? 'after' : 'before';
      }
      if (step.array_filters) o.arrayFilters = step.array_filters;
      if (op === 'find_one_and_delete') return await coll.findOneAndDelete(step.filter, o);
      const fn = op === 'find_one_and_update' ? 'findOneAndUpdate' : 'findOneAndReplace';
      return await coll[fn](step.filter, step.update, o);
    }
    case 'create_collection': await db.createCollection(step.coll); return 'ok';
    case 'drop_collection': await coll.drop(); return 'ok';
    case 'list_collection_names': return (await db.listCollections({}, { nameOnly: true }).toArray()).map((c) => c.name).sort();
    case 'list_collections': {
      const cs = await db.listCollections(step.filter ?? {}).toArray();
      return cs.map((c) => ({ name: c.name, type: c.type })).sort((a, b) => (a.name < b.name ? -1 : 1));
    }
    case 'create_index': {
      const keys = Object.fromEntries(step.keys.map(([k, d]) => [k, d]));
      const o = {};
      if (step.unique !== undefined) o.unique = step.unique;
      if (step.name !== undefined) o.name = step.name;
      Object.assign(o, step.opts ?? {});
      return await coll.createIndex(keys, o);
    }
    case 'drop_index': await coll.dropIndex(typeof step.index === 'string' ? step.index : Object.fromEntries(step.index.map(([k, d]) => [k, d]))); return 'ok';
    case 'list_indexes': return (await coll.listIndexes().toArray()).map((i) => (step.full ? i : Object.fromEntries(['v', 'key', 'name', 'unique'].filter((k) => k in i).map((k) => [k, i[k]]))));
    case 'command': case 'command_raw': return pick(await db.command(swapDb(step.cmd, dbname), so), step.pick, step.reduce);
    case 'admin_command': return pick(await ctx.client.db('admin').command(swapDb(step.cmd, dbname)), step.pick);
    case 'db_in_list': return (await ctx.client.db().admin().listDatabases({ nameOnly: true })).databases.some((d) => d.name === dbname);
    case 'db_in_list_command': return (await ctx.client.db('admin').command({ listDatabases: 1, nameOnly: true })).databases.some((d) => d.name === dbname);
    case 'drop_database': await db.dropDatabase(); return 'ok';
    case 'session_insert': {
      const s = ctx.client.startSession();
      try { await coll.insertOne({ _id: 1 }, { session: s }); return await coll.find({}, { session: s }).toArray(); } finally { await s.endSession(); }
    }
    case 'ping_with_server_api': {
      const c = new MongoClient(ctx.uri, { serverApi: { version: '1' }, serverSelectionTimeoutMS: 15000 });
      try { await c.connect(); return (await c.db('admin').command({ ping: 1 })).ok; } finally { await c.close(); }
    }
    case 'raw_op_query': return await rawOpQuery(ctx);
    case 'insert_one_w0': {
      const r = await ctx.client.db(dbname).collection(step.coll, { writeConcern: { w: 0 } }).insertOne(step.doc);
      return { acknowledged: r.acknowledged };
    }
    case 'client_with_compression': {
      const c = new MongoClient(ctx.uri, { compressors: ['zlib'], serverSelectionTimeoutMS: 15000, promoteValues: false });
      try {
        await c.db(dbname).collection(step.coll).insertOne({ _id: 1, v: 'x'.repeat(2000) });
        return await c.db(dbname).collection(step.coll).find().toArray();
      } finally { await c.close(); }
    }
    case 'cursor_batches': {
      const cmd = { find: step.coll, sort: step.sort };
      if (step.batch_size !== undefined) cmd.batchSize = step.batch_size;
      return await rawBatches(db, step.coll, await db.command(cmd));
    }
    case 'cursor_batches_agg': return await rawBatches(db, step.coll, await db.command({ aggregate: step.coll, pipeline: step.pipeline, cursor: { batchSize: step.batch_size } }));
    case 'cursor_kill_after': {
      const cur = coll.find({}, { sort: { _id: 1 }, batchSize: num(step.batch_size) });
      const take = num(step.take);
      for (let i = 0; i < take; i++) await cur.next();
      await cur.close();
      return { taken: take, alive: !cur.closed };
    }
    case 'cursor_close_midway': {
      const cur = coll.find({}, { sort: { _id: 1 }, batchSize: num(step.batch_size) });
      const got = [];
      for (let i = 0; i < num(step.take); i++) got.push(num((await cur.next())._id));
      if (ctx.isOurs) await db.command({ tenantAdmin: 1, close: true });
      for await (const d of cur) got.push(num(d._id));
      return { n: got.length, ids_ok: got.every((v, i) => v === i) && got.length === 300 };
    }
    case 'getmore_bogus': return await db.command({ getMore: Long.fromNumber(123456789), collection: step.coll });
    case 'sleep': await sleep(num(step.secs) * 1000); return SKIP;
    case 'tenant_close': if (ctx.isOurs) await db.command({ tenantAdmin: 1, close: true }); return SKIP;
    case 'expect_mode': {
      if (ctx.isOurs) {
        const got = (await db.command({ tenantAdmin: 1 })).mode;
        if (got !== step.mode) ctx.asserts.push(`expected tenant mode '${step.mode}', found '${got}'`);
      }
      return SKIP;
    }
    case 'restart_server': {
      if (ctx.isOurs) {
        const pid = parseInt(fs.readFileSync(path.join(HERE, 'out', `server-${ctx.port}.pid`), 'utf8'), 10);
        if (step.kill) { process.kill(pid, 'SIGKILL'); await sleep(500); }
        execFileSync(path.join(HERE, 'server.sh'), [String(ctx.port), String(ctx.idleSecs)], { stdio: 'ignore' });
        await ctx.reconnect();
      }
      return SKIP;
    }
    case 'parallel_inserts': {
      const errs = [];
      await Promise.all(Array.from({ length: num(step.workers) }, async (_, w) => {
        try { for (let i = 0; i < num(step.per_worker); i++) await coll.insertOne({ _id: `${w}-${i}`, w, i }); } catch (e) { errs.push(String(e)); }
      }));
      return { errors: errs };
    }
    case 'parallel_inc': {
      const errs = [];
      await Promise.all(Array.from({ length: num(step.workers) }, async () => {
        try { for (let i = 0; i < num(step.per_worker); i++) await coll.updateOne({ _id: 1 }, { $inc: { n: 1 } }); } catch (e) { errs.push(String(e)); }
      }));
      return { errors: errs };
    }
    case 'parallel_dbs': {
      const n = num(step.dbs), per = num(step.per_db);
      const counts = await Promise.all(Array.from({ length: n }, async (_, k) => {
        const d = ctx.client.db(`${dbname}_${k}`);
        await d.collection('c').insertMany(Array.from({ length: per }, (_, i) => ({ _id: i })));
        return await d.collection('c').countDocuments({});
      }));
      for (let k = 0; k < n; k++) await ctx.client.db(`${dbname}_${k}`).dropDatabase();
      return counts;
    }
    case 'parallel_read_write': {
      const bad = [];
      const writers = Array.from({ length: 4 }, async (_, w) => { for (let i = 0; i < 30; i++) await coll.insertOne({ _id: 1000 + w * 100 + i }); });
      const readers = Array.from({ length: 4 }, async () => { for (let i = 0; i < 30; i++) if (num(await coll.countDocuments({})) < 100) bad.push('count dropped below 100'); });
      await Promise.all([...writers, ...readers]);
      return { bad, final: await coll.countDocuments({}) };
    }
    default: throw new Error(`unknown op ${op}`);
  }
}

async function runScript(ctx, test, dbname) {
  const results = [];
  ctx.asserts = [];
  for (const step of unescape(clone(test.steps))) {
    let r;
    try { r = await execStep(ctx, ctx.client.db(dbname), dbname, step); } catch (e) { r = errResult(e, step.cmp_msg ?? false); }
    if (r !== SKIP) results.push(r);
  }
  return results;
}

function normalize(x, keepOids, sortKeys) {
  if (x === null || x === undefined) return x;
  if (x instanceof ObjectId) return keepOids ? x : '<oid>';
  if (x._bsontype || x instanceof Date || x instanceof Uint8Array) return x;
  if (Array.isArray(x)) return x.map((v) => normalize(v, keepOids, sortKeys));
  if (typeof x === 'object') {
    const keys = sortKeys ? Object.keys(x).sort() : Object.keys(x);
    return Object.fromEntries(keys.map((k) => [k, normalize(x[k], keepOids, sortKeys)]));
  }
  return x;
}
function canon(x, test) {
  x = normalize(x, test.keep_oids ?? false, test.sort_keys ?? false);
  if (test.unordered && Array.isArray(x)) x = [...x].sort((a, b) => { const sa = EJSON.stringify(normalize(a, true, true), { relaxed: false }), sb = EJSON.stringify(normalize(b, true, true), { relaxed: false }); return sa < sb ? -1 : sa > sb ? 1 : 0; });
  return EJSON.stringify(x === undefined ? null : x, { relaxed: false });
}
function compare(test, real, ours) {
  if (real.length !== ours.length) return { step: Math.min(real.length, ours.length), real: `${real.length} results`, ours: `${ours.length} results` };
  for (let i = 0; i < real.length; i++) {
    const a = canon(real[i], test), b = canon(ours[i], test);
    if (a !== b) return { step: i, op: test.steps[Math.min(i, test.steps.length - 1)].op, real: a.slice(0, 600), ours: b.slice(0, 600) };
  }
  return null;
}

async function main() {
  const args = Object.fromEntries(process.argv.slice(2).map((a) => a.replace(/^--/, '')).map((a) => { const i = a.indexOf('='); return i < 0 ? [a, true] : [a.slice(0, i), a.slice(i + 1)]; }));
  const testsPath = args.tests || path.join(HERE, 'tests.json');
  let tests = EJSON.parse(fs.readFileSync(testsPath, 'utf8'), { relaxed: false });
  if (args.only) tests = tests.filter((t) => t.name.includes(args.only));
  if (args.category) tests = tests.filter((t) => t.category === args.category);
  const out = args.out || path.join(HERE, 'out', 'results_node.json');
  fs.mkdirSync(path.dirname(out), { recursive: true });
  const real = new Ctx(args.real || 'mongodb://localhost:27017', 27017, 0, false);
  const ours = new Ctx(args.ours || 'mongodb://localhost:27018', 27018, 60, true);
  const idle = new Ctx(args.idle || 'mongodb://localhost:27019', 27019, 3, true);
  await Promise.all([real.connect(), ours.connect(), idle.connect()]);
  const results = [];
  let n = 0;
  for (const t of tests) {
    const dbname = `dn${n++}_${Math.random().toString(16).slice(2, 10)}`;
    const ourCtx = t.idle ? idle : ours;
    const t0 = Date.now();
    let status, diff = null;
    try {
      const r = await runScript(real, t, dbname);
      const o = await runScript(ourCtx, t, dbname);
      diff = compare(t, r, o);
      if (ourCtx.asserts.length) diff = diff || { step: -1, real: '', ours: ourCtx.asserts.join('; ') };
      status = diff ? 'fail' : 'pass';
    } catch (e) { status = 'error'; diff = { step: -1, real: '', ours: `harness error: ${e && e.stack || e}` }; }
    for (const c of [real, ourCtx]) { try { await c.client.db(dbname).dropDatabase(); } catch (e) { /* ignore */ } }
    results.push({ name: t.name, group: t.group, category: t.category, status, detail: diff, secs: (Date.now() - t0) / 1000 });
    console.log(status === 'pass' ? `. ${t.name}` : `F ${t.name}\n    step ${diff.step} ${diff.op || ''}\n    real: ${diff.real}\n    ours: ${diff.ours}`);
  }
  fs.writeFileSync(out, JSON.stringify(results, null, 1));
  for (const cat of ['core', 'extra', 'replset']) {
    const sel = results.filter((x) => x.category === cat);
    if (sel.length) console.log(`${cat}: ${sel.filter((x) => x.status === 'pass').length}/${sel.length} passed`);
  }
  await Promise.all([real.client.close(), ours.client.close(), idle.client.close()]);
}
main().catch((e) => { console.error(e); process.exit(1); });
