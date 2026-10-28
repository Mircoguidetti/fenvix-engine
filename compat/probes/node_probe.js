// Extra differential probes for Node.js driver (mongodb 7.7) members that mongo-tests/node/run_node.js never calls.
// Each case runs on real mongod and on the engine (fresh database each) and the EJSON results/errors are compared.
// usage: node node_probe.js [--only=substr] [--real=uri] [--real-rs=uri] [--ours=uri] [--diff]   -> ../probe_node_results.json
const path = require('path');
const fs = require('fs');
const { createRequire } = require('module');
const req = createRequire(path.join(__dirname, '../../mongo-tests/node/package.json'));
const { MongoClient, GridFSBucket, ObjectId, Long } = req('mongodb');
const { EJSON } = req('bson');

const args = Object.fromEntries(process.argv.slice(2).map((a) => a.replace(/^--/, '')).map((a) => { const i = a.indexOf('='); return i < 0 ? [a, true] : [a.slice(0, i), a.slice(i + 1)]; }));
const CASES = [];
const N = (cls, ...members) => members.map((m) => `node:${cls}.${m}`);
const C = (name, ids, fn, o = {}) => CASES.push({ name, ids, fn, rs: !!o.rs, fail: o.fail || 'partial', expectErr: !!o.expectErr });

const sortKeys = (x) => Array.isArray(x) ? x.map(sortKeys) : (x && typeof x === 'object' && !(x instanceof Date) && !x._bsontype && !Buffer.isBuffer(x)) ? Object.fromEntries(Object.keys(x).sort().map((k) => [k, sortKeys(x[k])])) : x;
const canon = (x) => EJSON.stringify(sortKeys(x === undefined ? null : x), { relaxed: false });
const errOf = (e) => ({ __error__: { code: e.code ?? null, codeName: e.codeName ?? null, name: e.code ? undefined : e.name } });
const seed = async (db, n = 10) => db.collection('t').insertMany(Array.from({ length: n }, (_, i) => ({ _id: i, a: i, g: i % 3, s: `s${i}`, arr: [i, i + 1] })));
const ids = (docs) => docs.map((d) => d._id ?? d.a);

// ------------------------------------------------------------------ Collection
C('node.Collection.bulkWrite', N('Collection', 'bulkWrite'), async (c, db) => { await seed(db, 6); const r = await db.collection('t').bulkWrite([{ insertOne: { document: { _id: 100 } } }, { updateOne: { filter: { _id: 1 }, update: { $set: { z: 1 } } } }, { updateMany: { filter: { g: 0 }, update: { $inc: { a: 10 } } } }, { replaceOne: { filter: { _id: 2 }, replacement: { r: 1 } } }, { deleteOne: { filter: { _id: 3 } } }, { deleteMany: { filter: { g: 1 } } }]); return { r: { i: r.insertedCount, m: r.matchedCount, mo: r.modifiedCount, d: r.deletedCount }, docs: await db.collection('t').find().sort({ _id: 1 }).toArray() }; });
C('node.Collection.count', N('Collection', 'count'), async (c, db) => { await seed(db); return [await db.collection('t').count({ a: { $gt: 4 } }), await db.collection('t').count()]; });
C('node.Collection.createIndexes_indexes_indexExists', N('Collection', 'createIndexes', 'indexes', 'indexExists', 'indexInformation'), async (c, db) => { await seed(db); const r = await db.collection('t').createIndexes([{ key: { a: 1 }, name: 'a_1' }, { key: { g: -1 }, name: 'g_1' }]); return { r, ix: (await db.collection('t').indexes()).map((i) => i.name).sort(), exists: await db.collection('t').indexExists('a_1'), no: await db.collection('t').indexExists('zzz'), info: Object.keys(await db.collection('t').indexInformation()).sort() }; });
C('node.Collection.dropIndexes', N('Collection', 'dropIndexes'), async (c, db) => { await seed(db); await db.collection('t').createIndex({ a: 1 }); await db.collection('t').dropIndexes(); return (await db.collection('t').indexes()).map((i) => i.name); });
C('node.Collection.options_isCapped', N('Collection', 'options', 'isCapped'), async (c, db) => { await db.createCollection('cap', { capped: true, size: 4096 }); return { opts: await db.collection('cap').options(), capped: await db.collection('cap').isCapped() }; }, { fail: 'missing' });
C('node.Collection.rename', N('Collection', 'rename'), async (c, db) => { await seed(db); await db.collection('t').rename('t2'); return { cols: (await db.listCollections({}, { nameOnly: true }).toArray()).map((x) => x.name).sort(), n: await db.collection('t2').countDocuments() }; });
C('node.Collection.bulkOps', N('Collection', 'initializeOrderedBulkOp', 'initializeUnorderedBulkOp').concat(N('BulkOperationBase', 'execute', 'find', 'insert', 'raw')), async (c, db) => { await seed(db, 4); const o = db.collection('t').initializeOrderedBulkOp(); o.insert({ _id: 50 }); o.find({ _id: 1 }).updateOne({ $set: { k: 1 } }); o.find({ _id: 2 }).deleteOne(); o.find({ _id: 99 }).upsert().replaceOne({ up: 1 }); o.raw({ insertOne: { document: { _id: 51 } } }); const r = await o.execute(); const u = db.collection('t').initializeUnorderedBulkOp(); u.insert({ _id: 0 }); u.insert({ _id: 52 }); let ue = null; try { await u.execute(); } catch (e) { ue = { code: e.code, n: e.result?.insertedCount }; } return { r: { i: r.insertedCount, m: r.matchedCount, d: r.deletedCount, u: r.upsertedCount }, ue, docs: await db.collection('t').find().sort({ _id: 1 }).toArray() }; });
C('node.Collection.createSearchIndex', N('Collection', 'createSearchIndex'), async (c, db) => { await seed(db); try { await db.collection('t').createSearchIndex({ name: 'x', definition: { mappings: { dynamic: true } } }); return 'created'; } catch (e) { return errOf(e); } }, { expectErr: true });

// ------------------------------------------------------------------ Db
C('node.Db.aggregate', N('Db', 'aggregate'), async (c, db) => db.aggregate([{ $documents: [{ x: 1 }, { x: 2 }] }, { $match: { x: { $gt: 1 } } }]).toArray());
C('node.Db.collections_dropCollection', N('Db', 'collections', 'dropCollection'), async (c, db) => { await seed(db); await db.collection('u').insertOne({ _id: 1 }); const before = (await db.collections()).map((x) => x.collectionName).sort(); const d = await db.dropCollection('u'); return { before, d, after: (await db.collections()).map((x) => x.collectionName).sort() }; });
C('node.Db.createIndex_indexInformation', N('Db', 'createIndex', 'indexInformation'), async (c, db) => { await seed(db); const n = await db.createIndex('t', { a: 1 }); return { n, info: Object.keys(await db.indexInformation('t')).sort() }; });
C('node.Db.renameCollection', N('Db', 'renameCollection'), async (c, db) => { await seed(db); await db.renameCollection('t', 't3'); return (await db.collections()).map((x) => x.collectionName).sort(); });
C('node.Db.runCursorCommand', N('Db', 'runCursorCommand'), async (c, db) => { await seed(db); return (await db.runCursorCommand({ find: 't', filter: { a: { $gt: 5 } }, sort: { a: 1 }, batchSize: 2 }).toArray()).map((d) => d.a); });
C('node.Db.stats', N('Db', 'stats'), async (c, db) => { await seed(db); const s = await db.stats(); return { db: s.db.replace(db.databaseName, '$DB'), objects: Number(s.objects), collections: Number(s.collections), ok: s.ok }; });
C('node.Db.profiling', N('Db', 'profilingLevel', 'setProfilingLevel'), async (c, db) => { const a = await db.profilingLevel(); await db.setProfilingLevel('off'); return a; }, { fail: 'missing' });
C('node.Db.removeUser', N('Db', 'removeUser'), async (c, db) => { try { return await db.removeUser('nosuchuser'); } catch (e) { return errOf(e); } }, { fail: 'missing' });
C('node.Db.watch', N('Db', 'watch'), async (c, db) => { const st = db.watch([], { maxAwaitTimeMS: 300 }); await st.tryNext(); await db.collection('c').insertOne({ _id: 1 }); let e = null; for (let i = 0; i < 6 && !e; i++) e = await st.tryNext(); await st.close(); return e && { op: e.operationType, coll: e.ns.coll }; }, { rs: true });
C('node.MongoClient.watch', N('MongoClient', 'watch'), async (c, db) => { const st = c.watch([], { maxAwaitTimeMS: 300 }); await st.tryNext(); await db.collection('c').insertOne({ _id: 1 }); let e = null; for (let i = 0; i < 10 && !(e && e.ns.db === db.databaseName); i++) e = await st.tryNext(); await st.close(); return e && { op: e.operationType, coll: e.ns.coll }; }, { rs: true });
C('node.ChangeStream.hasNext_stream', N('ChangeStream', 'hasNext'), async (c, db) => { const st = db.collection('c').watch([], { maxAwaitTimeMS: 300 }); await st.tryNext(); await db.collection('c').insertOne({ _id: 1 }); const h = await st.hasNext(); const e = await st.next(); await st.close(); return { h, op: e.operationType, closed: st.closed }; }, { rs: true });

// ------------------------------------------------------------------ MongoClient / Admin
C('node.MongoClient.connect_close', N('MongoClient', 'connect', 'close'), async (c, db) => { const m = new MongoClient(c.__uri, { directConnection: true }); await m.connect(); await m.db(db.databaseName).collection('t').insertOne({ _id: 1 }); await m.close(); return await db.collection('t').countDocuments(); });
C('node.MongoClient.bulkWrite', N('MongoClient', 'bulkWrite'), async (c, db) => { try { const r = await c.bulkWrite([{ name: 'insertOne', namespace: `${db.databaseName}.t`, document: { _id: 1 } }]); return { i: r.insertedCount }; } catch (e) { return errOf(e); } }, { expectErr: true });
C('node.Admin.members', N('Admin', 'buildInfo', 'command', 'listDatabases', 'ping', 'serverInfo', 'serverStatus', 'validateCollection', 'replSetGetStatus'), async (c, db) => { await seed(db); const a = c.db('admin').admin(); const out = {}; out.ping = (await a.ping()).ok; out.build = (await a.buildInfo()).version.slice(0, 3); out.info = (await a.serverInfo()).version.slice(0, 3); out.cmd = (await a.command({ ping: 1 })).ok; out.dbs = (await a.listDatabases({ nameOnly: true })).databases.some((d) => d.name === db.databaseName); out.val = (await a.validateCollection('t', {}).catch((e) => errOf(e))); for (const m of ['serverStatus', 'replSetGetStatus']) { try { out[m] = !!(await a[m]()).ok; } catch (e) { out[m] = errOf(e); } } return out; });

// ------------------------------------------------------------------ cursors
const cur = (name, members, fn, o) => C(`node.FindCursor.${name}`, members, async (c, db) => { await seed(db, 12); return fn(db.collection('t'), db); }, o);
cur('sort_limit_skip_project', N('FindCursor', 'sort', 'limit', 'skip', 'project', 'filter'), async (t) => ids(await t.find({}).filter({ a: { $gte: 2 } }).sort({ a: -1 }).skip(1).limit(4).project({ a: 1 }).toArray()));
cur('batchSize_clone_rewind', N('FindCursor', 'clone').concat(N('AbstractCursor', 'batchSize', 'clone', 'rewind', 'next', 'toArray', 'hasNext', 'tryNext', 'forEach', 'stream', 'close-1')), async (t) => { const c = t.find().sort({ a: 1 }).batchSize(3).limit(7); const a = ids(await c.toArray()); c.rewind(); const b = [await c.next(), await c.next()].map((d) => d.a); const hn = await c.hasNext(); const tn = (await c.tryNext())?.a; const seen = []; await c.forEach((d) => { seen.push(d.a); }); const k = ids(await c.clone().toArray()); const s = []; for await (const d of t.find().sort({ a: 1 }).limit(3).stream()) s.push(d.a); const cc = t.find().batchSize(2); await cc.next(); await cc.close(); return { a, b, hn, tn, seen, k, s, killed: cc.killed || cc.closed }; });
cur('hint_comment_maxTime_allowDiskUse', N('FindCursor', 'hint', 'comment', 'maxTimeMS', 'allowDiskUse'), async (t) => { await t.createIndex({ a: 1 }); const q = () => t.find({ a: { $gt: 3 } }); return { hint: ids(await q().hint({ a: 1 }).toArray()), comment: ids(await q().comment('c').toArray()), maxTime: ids(await q().maxTimeMS(5000).toArray()), disk: ids(await q().sort({ a: 1 }).allowDiskUse(true).toArray()) }; });
cur('count_explain', N('FindCursor', 'count', 'explain'), async (t) => { const e = await t.find({ a: 3 }).explain(); return { count: await t.find({ a: { $gt: 5 } }).count(), keys: Object.keys(e).filter((k) => ['queryPlanner', 'ok'].includes(k)).sort() }; });
cur('collation', N('FindCursor', 'collation'), async (t) => ids(await t.find({ s: 'S3' }).collation({ locale: 'en', strength: 2 }).toArray()), { fail: 'missing' });
cur('min_max_returnKey_showRecordId', N('FindCursor', 'min', 'max', 'returnKey', 'showRecordId'), async (t) => { await t.createIndex({ a: 1 }); return { mm: ids(await t.find().hint({ a: 1 }).min({ a: 3 }).max({ a: 7 }).toArray()), rk: await t.find().hint({ a: 1 }).returnKey(true).sort({ a: 1 }).limit(2).toArray(), rid: Object.keys((await t.find().showRecordId(true).limit(1).toArray())[0]).sort() }; }, { fail: 'missing' });
const agg = (name, members, fn) => C(`node.AggregationCursor.${name}`, members, async (c, db) => { await seed(db, 12); return fn(db.collection('t'), db); });
agg('builders', N('AggregationCursor', 'match', 'group', 'project', 'sort', 'limit', 'skip', 'unwind', 'addStage', 'lookup'), async (t) => (await t.aggregate().match({ a: { $gte: 1 } }).group({ _id: '$g', n: { $sum: 1 }, arr: { $push: '$arr' } }).project({ n: 1, arr: 1 }).sort({ _id: 1 }).skip(0).limit(5).unwind('$arr').addStage({ $limit: 4 }).lookup({ from: 't', localField: '_id', foreignField: 'g', as: 'l' }).limit(3).toArray()).map((d) => ({ id: d._id, n: d.n, l: d.l.length })));
agg('out_redact_geoNear_clone_explain', N('AggregationCursor', 'out', 'redact', 'clone', 'explain'), async (t, db) => { const r = await t.aggregate().redact({ $cond: [{ $gt: ['$a', 5] }, '$$KEEP', '$$PRUNE'] }).sort({ _id: 1 }).toArray(); await t.aggregate().match({ a: { $lt: 3 } }).out('o').toArray(); const ex = await t.aggregate().match({ a: 1 }).explain(); const k = t.aggregate().match({}).clone(); return { r: ids(r), o: (await db.collection('o').find().sort({ _id: 1 }).toArray()).length, ex: Object.keys(ex).includes('queryPlanner') || Object.keys(ex).includes('stages'), k: (await k.toArray()).length }; });

// ------------------------------------------------------------------ GridFS
C('node.GridFSBucket.roundtrip', N('GridFSBucket', 'openUploadStream', 'openUploadStreamWithId', 'openDownloadStream', 'openDownloadStreamByName', 'find', 'rename', 'delete', 'drop').concat(N('GridFSBucketWriteStream', 'abort')), async (c, db) => {
  const b = new GridFSBucket(db, { chunkSizeBytes: 4 });
  const up = b.openUploadStream('a.txt', { metadata: { k: 1 } }); await new Promise((res, rej) => { up.on('finish', res); up.on('error', rej); up.end(Buffer.from('hello node gridfs')); }); const id = up.id;
  const read = async (s) => { const ch = []; for await (const x of s) ch.push(x); return Buffer.concat(ch).toString(); };
  const out = { data: await read(b.openDownloadStream(id)), byName: await read(b.openDownloadStreamByName('a.txt')), files: (await b.find({ filename: 'a.txt' }).toArray()).map((f) => ({ len: f.length, cs: f.chunkSize, md: f.metadata, fn: f.filename })) };
  const rd = b.openDownloadStream(id, { start: 6, end: 10 }); out.range = await read(rd);
  await b.rename(id, 'b.txt'); out.renamed = (await b.find().toArray()).map((f) => f.filename);
  const id2 = new ObjectId(); const up2 = b.openUploadStreamWithId(id2, 'w.bin'); await new Promise((res) => { up2.on('finish', res); up2.end(Buffer.alloc(10, 7)); }); out.withId = String(up2.id) === String(id2);
  const up3 = b.openUploadStream('abort.bin'); up3.on('error', () => {}); up3.write(Buffer.alloc(20, 1)); try { await up3.abort(); } catch (e) { out.abortErr = e.name; } out.afterAbort = (await b.find({ filename: 'abort.bin' }).toArray()).length;
  await b.delete(id); out.afterDelete = (await b.find().toArray()).map((f) => f.filename).sort();
  await b.drop(); out.cols = (await db.listCollections({}, { nameOnly: true }).toArray()).map((x) => x.name).sort(); return out; });

// ------------------------------------------------------------------ sessions
C('node.ClientSession.members', N('ClientSession', 'id', 'inTransaction', 'hasEnded', 'clusterTime', 'operationTime'), async (c, db) => { const s = c.startSession(); await db.collection('t').insertOne({ _id: 1 }, { session: s }); const o = { hasId: !!s.id, inTx: s.inTransaction(), ended: s.hasEnded, ct: s.clusterTime != null, ot: s.operationTime != null }; await s.endSession(); o.ended2 = s.hasEnded; return o; }, { rs: true });

// ------------------------------------------------------------------ runner
async function runOn(client, c) {
  const dbname = 'np_' + Math.random().toString(16).slice(2, 12);
  const db = client.db(dbname);
  try { return canon(await Promise.race([c.fn(client, db), new Promise((_, rej) => setTimeout(() => rej(Object.assign(new Error('probe timeout 25s'), { code: 'TIMEOUT' })), 25000))])).split(dbname).join('$DB'); } catch (e) { return canon(errOf(e)); } finally { try { await db.dropDatabase(); } catch (e) { /* ignore */ } }
}
const deepDiff = (a, b, p = '') => { const out = []; if (a && b && typeof a === 'object' && typeof b === 'object') { for (const k of new Set([...Object.keys(a), ...Object.keys(b)])) { if (!(k in a)) out.push(`${p}/${k}: only ours`); else if (!(k in b)) out.push(`${p}/${k}: only real`); else out.push(...deepDiff(a[k], b[k], `${p}/${k}`)); } } else if (JSON.stringify(a) !== JSON.stringify(b)) out.push(`${p}: real=${JSON.stringify(a)?.slice(0, 80)} ours=${JSON.stringify(b)?.slice(0, 80)}`); return out; };
(async () => {
  const mk = async (uri) => { const c = await new MongoClient(uri).connect(); c.__uri = uri; return c; };
  const real = await mk(args.real || 'mongodb://localhost:27017');
  const realRs = await mk(args['real-rs'] || 'mongodb://localhost:27020/?replicaSet=rs0');
  const oursRs = await mk(args.ours || 'mongodb://localhost:27018');
  const oursSa = await mk(args['ours-sa'] || 'mongodb://localhost:27019');   // (standalone engine vs standalone mongod; replica-set probes: rs engine vs rs mongod)
  const results = {}; const byId = {};
  for (const c of CASES) {
    if (args.only && !c.name.includes(args.only)) continue;
    let r = await runOn(c.rs ? realRs : real, c); let o = await runOn(c.rs ? oursRs : oursSa, c);
    const r2 = await runOn(c.rs ? realRs : real, c); const o2 = await runOn(c.rs ? oursRs : oursSa, c);
    const ok = r === o; const rerr = r.includes('__error__'); const oerr = o.includes('__error__');
    let kind = ok ? '' : (oerr && !rerr ? 'reject' : 'differ'); let status = ok ? 'pass' : 'fail';
    if (ok && rerr && oerr && !c.expectErr) { status = 'invalid'; kind = 'invalid'; }
    if ((r2 === o2) !== ok) { status = 'flaky'; kind = 'flaky'; }
    results[c.name] = { ids: c.ids, status, kind, fail: c.fail, real: r.slice(0, 300), ours: o.slice(0, 300) };
    for (const id of c.ids) (byId[id] ||= []).push({ name: c.name, status, kind, fail: c.fail });
    console.log(status === 'pass' ? '.' : status === 'invalid' ? 'I' : 'F', c.name);
    if (status !== 'pass') { console.log('    real:', r.slice(0, 220), '\n    ours:', o.slice(0, 220)); if (args.diff) for (const d of deepDiff(JSON.parse(r), JSON.parse(o)).slice(0, 10)) console.log('      diff', d); }
  }
  if (!args.only) fs.writeFileSync(path.join(__dirname, '..', 'gaps_status', 'probe_node_results_final.json'), JSON.stringify({ cases: results, by_id: byId }, null, 1));
  const n = Object.values(results).filter((x) => x.status === 'pass').length;
  console.log(`${n}/${Object.keys(results).length} node probes identical on both servers`);
  await Promise.all([real.close(), realRs.close(), oursRs.close(), oursSa.close()]);
})().catch((e) => { console.error(e); process.exit(1); });
