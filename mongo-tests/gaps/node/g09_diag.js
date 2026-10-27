// Item 9 (Node driver): diagnostics and administration commands (see g09_diag.py).
module.exports = ({ C, Long, ObjectId }) => {
  const clean = (m) => (m || '').replace(/UUID\("[0-9a-f-]+"\)/g, 'UUID(<u>)');
  const tr = async (f) => { try { const r = await f(); if (r && typeof r === 'object' && !Array.isArray(r)) { delete r.$clusterTime; delete r.operationTime; } return r; } catch (e) { return { err: [e.code, e.codeName, clean(e.errmsg || e.message)] }; } };
  const shape = (v) => {
    if (Array.isArray(v)) return v.length ? [shape(v[0])] : [];
    if (v && v._bsontype) return v._bsontype;
    if (v instanceof Date) return 'datetime';
    if (v && typeof v === 'object') return Object.fromEntries(Object.entries(v).filter(([k]) => !['$clusterTime', 'operationTime'].includes(k)).map(([k, x]) => [k, shape(x)]));
    return typeof v === 'number' ? (Number.isInteger(v) ? 'int' : 'float') : typeof v;
  };
  const cmd = (db, c) => tr(() => db.command(c));
  const adm = (c) => c.db('admin');
  const sleep = (ms) => new Promise((r) => setTimeout(r, ms));
  const sa = { sa: true };
  const rs = { rs: true };
  const num = (v) => (v && v._bsontype ? Number(v.toString()) : v);

  C('g09.diag.serverstatus_shape', async (c, db) => { const r = await adm(c).command({ serverStatus: 1 }); delete r.$clusterTime; delete r.operationTime; const cm = r.metrics.commands; delete r.metrics.commands; delete r.metrics.apiVersions; for (const k of ['locks', 'defaultRWConcern', 'transactions', 'wiredTiger', 'tcmalloc']) delete r[k]; return { shape: shape(r), entries: [...new Set(Object.values(cm).filter((v) => v && typeof v === 'object' && !v._bsontype).map((v) => JSON.stringify(Object.keys(v).filter((k) => ['failed', 'total'].includes(k)).sort())))].sort() }; }, sa);
  C('g09.diag.serverstatus_shape_rs', async (c, db) => { const r = await adm(c).command({ serverStatus: 1 }); delete r.$clusterTime; delete r.operationTime; const cm = r.metrics.commands; delete r.metrics.commands; delete r.metrics.apiVersions; for (const k of ['locks', 'defaultRWConcern', 'transactions', 'wiredTiger', 'tcmalloc']) delete r[k]; return { shape: shape(r), entries: [...new Set(Object.values(cm).filter((v) => v && typeof v === 'object' && !v._bsontype).map((v) => JSON.stringify(Object.keys(v).filter((k) => ['failed', 'total'].includes(k)).sort())))].sort() }; }, rs);
  C('g09.diag.serverstatus_counters', async (c, db) => {
    const a = await adm(c).command({ serverStatus: 1 }); const t = db.collection('t');
    await t.insertMany([0, 1, 2].map((_id) => ({ _id }))); await t.insertOne({ _id: 10 }); await t.find({}).toArray(); await t.updateOne({ _id: 1 }, { $set: { x: 1 } }); await t.updateMany({}, { $set: { y: 1 } }); await t.deleteOne({ _id: 10 });
    await t.aggregate([{ $match: {} }]).toArray(); await t.countDocuments({}); await t.find({}).batchSize(1).toArray();
    const b = await adm(c).command({ serverStatus: 1 }); const d = (path) => path.split('.').reduce((x, p) => x[p], b) - path.split('.').reduce((x, p) => x[p], a);
    const out = {}; for (const k of ['insert', 'query', 'update', 'delete', 'getmore', 'command']) out[k] = num(d('opcounters.' + k)); for (const k of ['inserted', 'deleted', 'updated', 'returned']) out[k] = num(d('metrics.document.' + k));
    for (const k of ['insert', 'find', 'update', 'delete', 'aggregate', 'getMore']) out['cmd.' + k] = num((b.metrics.commands[k] || { total: 0 }).total) - num((a.metrics.commands[k] || { total: 0 }).total);
    out.connections = [b.connections.current >= 1, b.connections.available > 0, num(b.uptimeMillis) >= num(a.uptimeMillis), num(b.pid) > 0, b.version, b.process]; return out;
  }, sa);
  C('g09.diag.serverstatus_failed_counter', async (c, db) => { const a = (await adm(c).command({ serverStatus: 1 })).metrics.commands; await cmd(db, { collMod: 'nope', validator: {} }); await cmd(db, { find: 't', filter: { $bogus: 1 } }); const b = (await adm(c).command({ serverStatus: 1 })).metrics.commands; const o = {}; for (const k of ['collMod', 'find']) o[k] = [num(b[k].total) - num((a[k] || { total: 0 }).total), num(b[k].failed) - num((a[k] || { failed: 0 }).failed)]; return o; }, sa);
  C('g09.diag.serverstatus_sections', async (c, db) => { const strip = (r) => Object.keys(r).filter((k) => !['$clusterTime', 'operationTime'].includes(k)).sort(); return { keys: strip(await adm(c).command({ serverStatus: 1, repl: 0, metrics: 0, locks: 0, wiredTiger: 0, mem: 0, network: 0, opcounters: 0 })), keys2: strip(await adm(c).command({ serverStatus: 1, connections: 0 })) }; }, sa);
  C('g09.diag.static_shapes', async (c, db) => {
    const out = {}; for (const n of ['hostInfo', 'connPoolStats', 'features', 'lockInfo']) out[n] = shape(await adm(c).command({ [n]: 1 }));
    out.hostInfo_other_db = shape(await db.command({ hostInfo: 1 })); out.serverStatus_other_db = Object.keys(await db.command({ serverStatus: 1 })).sort().slice(0, 6); out.getCmdLineOpts = Object.keys(await adm(c).command({ getCmdLineOpts: 1 })).sort();
    out.admin_only = {}; for (const n of ['getCmdLineOpts', 'currentOp', 'top', 'lockInfo', 'getLog']) out.admin_only[n] = await cmd(db, { [n]: 1 }); return out;
  }, sa);
  C('g09.diag.list_commands', async (c, db) => { const r = (await adm(c).command({ listCommands: 1 })).commands; const ours = ['find', 'insert', 'update', 'delete', 'aggregate', 'createIndexes', 'serverStatus', 'listCommands', 'currentOp', 'dbHash', 'profile', 'collMod', 'hello', 'ping', 'killOp']; return { entries: Object.fromEntries(ours.filter((k) => k in r).sort().map((k) => [k, r[k]])), has_find: 'find' in r }; }, sa);
  C('g09.diag.get_log', async (c, db) => { const g = await adm(c).command({ getLog: 'global' }); return { keys: Object.keys(g).sort(), log_is_list: Array.isArray(g.log), lines_are_json: g.log.slice(0, 5).every((l) => l.startsWith('{')), bad: await cmd(adm(c), { getLog: 'nope' }), bad_type: await cmd(adm(c), { getLog: 1 }), other_db: await cmd(db, { getLog: 'global' }), names: (await adm(c).command({ getLog: '*' })).names.includes('global') }; }, sa);
  C('g09.diag.current_op', async (c, db) => {
    const r = await adm(c).command({ currentOp: 1, 'command.currentOp': { $exists: true } }); const op = r.inprog[0];
    return { keys: Object.keys(op).sort(), op: op.op, ns: op.ns, type: op.type, active: op.active, command_keys: Object.keys(op.command).sort(), desc: op.desc.startsWith('conn'), client: /^127\.0\.0\.1:\d+$/.test(op.client), metadata: Object.keys(op.clientMetadata || {}).sort(),
      filter_none: (await adm(c).command({ currentOp: 1, ns: 'nope.x' })).inprog, other_db: await cmd(db, { currentOp: 1 }), all: (await adm(c).command({ currentOp: 1, $all: true })).inprog.length > 0 };
  }, sa);
  C('g09.diag.kill_op', async (c, db) => {
    await db.collection('t').insertMany(Array.from({ length: 20 }, (_, i) => ({ _id: i, v: i })));
    const slow = db.collection('t').find({ $where: 'function(){ sleep(2000); return true; }' }).toArray().then(() => 'finished', (e) => [e.code, e.codeName]);
    let opid = null; let ops = [];
    for (let i = 0; i < 40 && opid === null; i++) { await sleep(100); ops = (await adm(c).command({ currentOp: 1, ns: `${db.databaseName}.t` })).inprog; if (ops.length) opid = ops[0].opid; }
    const out = { found: opid !== null };
    if (opid !== null) { out.found_op = [ops[0].op, ops[0].command.find, ops[0].secs_running >= 0, num(ops[0].microsecs_running) > 0]; out.kill = await adm(c).command({ killOp: 1, op: opid }); }
    out.result = await slow; out.nonexistent = await adm(c).command({ killOp: 1, op: 99999999 });
    out.errors = { no_op: await cmd(adm(c), { killOp: 1 }), str_op: await cmd(adm(c), { killOp: 1, op: 'shard:1' }), other_db: await cmd(db, { killOp: 1, op: 1 }) }; return out;
  }, sa);
  C('g09.diag.top', async (c, db) => {
    const t = db.collection('t'); await t.insertMany([0, 1, 2, 3, 4].map((_id) => ({ _id }))); await t.insertOne({ _id: 9 }); await t.find({}).toArray(); await t.find({}).toArray(); await t.updateOne({ _id: 1 }, { $set: { a: 1 } }); await t.deleteOne({ _id: 9 }); await t.find({}).batchSize(2).toArray(); await t.countDocuments({});
    const r = (await adm(c).command({ top: 1 })).totals; const mine = r[`${db.databaseName}.t`]; const keys = Object.keys(r);
    return { note: r.note, keys: Object.keys(mine).sort(), cell: shape(mine.total), counts: Object.fromEntries(Object.entries(mine).map(([k, v]) => [k, v.count])), ns_sorted: JSON.stringify(keys) === JSON.stringify(['note', ...keys.filter((k) => k !== 'note').sort()]), other_db: await cmd(db, { top: 1 }) };
  }, sa);
  C('g09.diag.rwc_standalone', async (c, db) => ({ get: await cmd(adm(c), { getDefaultRWConcern: 1 }), set: await cmd(adm(c), { setDefaultRWConcern: 1, defaultWriteConcern: { w: 1 } }), cp_get: await cmd(adm(c), { getClusterParameter: '*' }), cp_set: await cmd(adm(c), { setClusterParameter: { changeStreamOptions: { preAndPostImages: { expireAfterSeconds: 60 } } } }) }), sa);
  const dropT = (r) => (r.err ? r : Object.fromEntries(Object.entries(r).filter(([k]) => !['updateOpTime', 'updateWallClockTime', 'localUpdateWallClockTime', '$clusterTime', 'operationTime'].includes(k))));
  C('g09.diag.rwc_replset', async (c, db) => {
    const A = adm(c); const out = {};
    await A.command({ setDefaultRWConcern: 1, defaultWriteConcern: { w: 1 } }); await A.command({ setDefaultRWConcern: 1, defaultReadConcern: {} });
    const a = await A.command({ getDefaultRWConcern: 1 }); out.initial = shape(a); out.initial_vals = { defaultReadConcern: a.defaultReadConcern, defaultWriteConcern: a.defaultWriteConcern, ws: a.defaultWriteConcernSource, rs: a.defaultReadConcernSource };
    out.set_w1 = dropT(await cmd(A, { setDefaultRWConcern: 1, defaultWriteConcern: { w: 1 } })); out.set_rc = dropT(await cmd(A, { setDefaultRWConcern: 1, defaultReadConcern: { level: 'majority' } }));
    const cases = { none: {}, w0: { defaultWriteConcern: { w: 0 } }, no_w: { defaultWriteConcern: { j: true } }, level_bad: { defaultReadConcern: { level: 'snapshot' } }, no_level: { defaultReadConcern: { foo: 1 } }, wc_str: { defaultWriteConcern: 'x' }, unset: { defaultWriteConcern: {}, defaultReadConcern: {} }, unset_rc: { defaultReadConcern: {} }, in_memory: { defaultReadConcern: { level: 'available' } }, wc_j: { defaultWriteConcern: { w: 'majority', j: true, wtimeout: 100 } } };
    for (const [n, s] of Object.entries(cases)) out[n] = dropT(await cmd(A, { setDefaultRWConcern: 1, ...s }));
    out.get_in_memory = dropT(await A.command({ getDefaultRWConcern: 1, inMemory: true })); await A.command({ setDefaultRWConcern: 1, defaultReadConcern: {} }); return out;
  }, rs);
  C('g09.diag.cluster_parameter_replset', async (c, db) => {
    const A = adm(c); const out = {};
    out.all = shape(await A.command({ getClusterParameter: '*' })); out.one = shape(await A.command({ getClusterParameter: 'changeStreamOptions' })); out.list = shape(await A.command({ getClusterParameter: ['changeStreamOptions'] })); out.unknown = await cmd(A, { getClusterParameter: 'nope' });
    out.set = await cmd(A, { setClusterParameter: { changeStreamOptions: { preAndPostImages: { expireAfterSeconds: 60 } } } }); const p = (await A.command({ getClusterParameter: 'changeStreamOptions' })).clusterParameters[0];
    out.after = { id: p._id, pp: p.preAndPostImages, time_set: p.clusterParameterTime.high > 0 };
    out.set_off = await cmd(A, { setClusterParameter: { changeStreamOptions: { preAndPostImages: { expireAfterSeconds: 'off' } } } }); out.set_unknown = await cmd(A, { setClusterParameter: { nope: { a: 1 } } }); out.set_bad = await cmd(A, { setClusterParameter: { changeStreamOptions: 5 } }); return out;
  }, rs);
  C('g09.diag.profile', async (c, db) => {
    const out = {}; const t = db.collection('t');
    await db.command({ profile: 0, slowms: 100, sampleRate: 1.0, filter: 'unset' });
    await t.insertMany([0, 1, 2, 3, 4].map((i) => ({ _id: i, a: i }))); out.get = await db.command({ profile: -1 }); out.l1 = await db.command({ profile: 1, slowms: 50 }); out.get1 = await db.command({ profile: -1 });
    await t.find({ a: 1 }).toArray(); out.fast_not_logged = (await db.listCollections({ name: 'system.profile' }).toArray()).length ? await db.collection('system.profile').countDocuments() : 0;
    out.l2 = await db.command({ profile: 2 }); await t.find({ a: 1 }).toArray(); await t.insertOne({ _id: 100 }); await t.updateOne({ _id: 1 }, { $set: { b: 1 } }); await t.deleteOne({ _id: 100 }); await t.aggregate([{ $match: {} }]).toArray();
    const prof = await db.collection('system.profile').find({ ns: `${db.databaseName}.t` }).sort({ ts: 1 }).toArray();
    out.ops = prof.map((p) => [p.op, p.ns, p.nreturned ?? null, p.ninserted ?? null, p.nMatched ?? null, p.nModified ?? null, p.ndeleted ?? null, Object.keys(p.command).sort().slice(0, 2), p.protocol ?? null, Number.isInteger(p.millis)]);
    out.keys = {}; for (const p of prof) out.keys[p.op + (p.command.aggregate ? '_agg' : '')] = Object.keys(p).sort();
    out.sysprof_opts = (await db.listCollections({ name: 'system.profile' }).toArray())[0].options;
    out.l3 = await db.command({ profile: 3 }); out.lneg = await cmd(db, { profile: -5 }); out.lstr = await cmd(db, { profile: 'x' }); out.sample = await db.command({ profile: 0, sampleRate: 0.5 }); out.sample_bad = await cmd(db, { profile: 0, sampleRate: 2 });
    out.filter = await db.command({ profile: 0, filter: { millis: { $gt: 100 } } }); out.get2 = await db.command({ profile: -1 }); out.filter_unset = await db.command({ profile: 0, filter: 'unset' }); out.off = await db.command({ profile: 0 });
    await db.command({ profile: 0, slowms: 100, sampleRate: 1.0, filter: 'unset' }); const n = await db.collection('system.profile').countDocuments(); await t.find({}).toArray(); out.off_no_more = (await db.collection('system.profile').countDocuments()) === n; return out;
  }, sa);
  C('g09.diag.profile_slowms', async (c, db) => {
    await db.collection('t').insertMany([0, 1, 2, 3, 4].map((_id) => ({ _id }))); await db.command({ profile: 0, slowms: 100, sampleRate: 1.0, filter: 'unset' });
    await db.command({ profile: 1, slowms: 0 }); await db.collection('t').find({}).toArray(); await db.command({ profile: 1, slowms: 100000 }); await db.collection('t').find({}).toArray(); await db.command({ profile: 0, slowms: 100 });
    return { count: await db.collection('system.profile').countDocuments({ op: 'query' }) };
  }, sa);
  C('g09.diag.dbhash', async (c, db) => {
    await db.collection('a').insertMany([0, 1, 2, 3, 4, 5].map((i) => ({ _id: i, p: 'x'.repeat(i) }))); await db.collection('b').insertMany([0, 1, 2, 3].map((i) => ({ _id: 'k' + i, n: [i, { z: i }] })));
    await db.command({ create: 'cap', capped: true, size: 4096 }); await db.collection('cap').insertMany([0, 1, 2, 3, 4].map((i) => ({ _id: 5 - i, v: i }))); await db.collection('c_empty').insertOne({ _id: 1 }); await db.collection('c_empty').deleteOne({ _id: 1 });
    const r = await db.command({ dbHash: 1 }); const out = { collections: r.collections, md5: r.md5, capped: r.capped, uuids: Object.keys(r.uuids).sort(), keys: Object.keys(r).sort(), host_is_str: typeof r.host === 'string' };
    const r2 = await db.command({ dbHash: 1, collections: ['a', 'cap'] }); out.subset = [r2.collections, r2.md5, r2.capped]; await db.collection('a').insertOne({ _id: 99 }); out.changed = (await db.command({ dbHash: 1 })).collections.a !== r.collections.a;
    const bad = await cmd(db, { dbHash: 1, collections: 'a' }); out.bad = { collections: bad.collections, capped: bad.capped, md5: bad.md5 }; out.empty_db = (await c.db('empty_' + db.databaseName).command({ dbHash: 1 })).collections; return out;
  }, sa);
  C('g09.diag.validate_db_metadata', async (c, db) => {
    const t = db.collection('t'); await t.insertOne({ _id: 1, a: 'x', b: 1 }); await t.createIndex({ a: 'text' }, { name: 'text_ix' }); await t.createIndex('b', { name: 'sparse_ix', sparse: true }); await t.createIndex('b', { name: 'partial_ix', partialFilterExpression: { b: { $gt: 0 } } }); await t.createIndex('a', { name: 'ttl_ix', expireAfterSeconds: 100 });
    const api = { version: '1', strict: true }; const A = adm(c);
    return { strict: await cmd(A, { validateDBMetadata: 1, apiParameters: api, db: db.databaseName }), non_strict: await cmd(A, { validateDBMetadata: 1, apiParameters: { version: '1' }, db: db.databaseName }), coll: await cmd(A, { validateDBMetadata: 1, apiParameters: api, db: db.databaseName, collection: 't' }),
      other_coll: await cmd(A, { validateDBMetadata: 1, apiParameters: api, db: db.databaseName, collection: 'zz' }), no_params: await cmd(A, { validateDBMetadata: 1 }), no_version: await cmd(A, { validateDBMetadata: 1, apiParameters: { strict: true } }), bad_strict: await cmd(A, { validateDBMetadata: 1, apiParameters: { version: '1', strict: 'x' } }) };
  }, sa);
  C('g09.diag.sessions', async (c, db) => {
    const A = adm(c); const r = await A.command({ startSession: 1 }); const sid = r.id; const out = { shape: shape(r), timeout: r.timeoutMinutes };
    out.use = (await db.command({ insert: 't', documents: [{ _id: 1 }], lsid: sid })).n; out.kill_all = await A.command({ killAllSessions: [] }); out.kill_pattern = await A.command({ killAllSessionsByPattern: [] }); out.bad = await cmd(A, { killAllSessions: 1 }); out.bad_pattern = await cmd(A, { killAllSessionsByPattern: 'x' });
    out.end = await A.command({ endSessions: [sid] }); out.kill_sessions = await A.command({ killSessions: [{ id: sid.id }] }); return out;
  }, sa);
  C('g09.diag.plan_cache', async (c, db) => {
    const t = db.collection('t'); await t.insertMany([0, 1, 2, 3, 4].map((i) => ({ _id: i, a: i }))); await t.createIndex('a'); const out = {}; out.list0 = await db.command({ planCacheListFilters: 't' });
    out.set = await cmd(db, { planCacheSetFilter: 't', query: { a: 1 }, indexes: [{ a: 1 }] }); out.list1 = await db.command({ planCacheListFilters: 't' }); out.set_same = await cmd(db, { planCacheSetFilter: 't', query: { a: 1 }, indexes: [{ a: 1 }] });
    out.set_other = await cmd(db, { planCacheSetFilter: 't', query: { a: 2 }, sort: { a: 1 }, indexes: [{ a: 1 }] }); out.list2 = (await db.command({ planCacheListFilters: 't' })).filters.length;
    out.no_indexes = await cmd(db, { planCacheSetFilter: 't', query: { a: 1 } }); out.no_query = await cmd(db, { planCacheSetFilter: 't', indexes: [{ a: 1 }] }); out.empty_indexes = await cmd(db, { planCacheSetFilter: 't', query: { a: 1 }, indexes: [] });
    out.clear_one = await cmd(db, { planCacheClearFilters: 't', query: { a: 1 } }); out.list3 = (await db.command({ planCacheListFilters: 't' })).filters.length; out.clear_all = await cmd(db, { planCacheClearFilters: 't' }); out.list4 = (await db.command({ planCacheListFilters: 't' })).filters;
    out.clear_plan = await cmd(db, { planCacheClear: 't' }); out.missing_coll = [await cmd(db, { planCacheClear: 'nope' }), await cmd(db, { planCacheListFilters: 'nope' }), await cmd(db, { planCacheSetFilter: 'nope', query: { a: 1 }, indexes: [{ a: 1 }] })];
    out.stats = await tr(() => t.aggregate([{ $planCacheStats: {} }]).toArray()); out.bad_name = await cmd(db, { planCacheClear: 5 }); return out;
  }, sa);
  C('g09.diag.reindex_compact', async (c, db) => {
    const t = db.collection('t'); await t.insertMany([0, 1, 2, 3, 4].map((i) => ({ _id: i, a: i }))); await t.createIndex('a', { name: 'a_1' }); await t.createIndex({ a: -1, _id: 1 }, { name: 'a_id', unique: true });
    const r = await db.command({ reIndex: 't' }); return { reindex: [r.nIndexesWas, r.nIndexes, r.indexes.map((i) => i.name), Object.keys(r).sort()], missing: await cmd(db, { reIndex: 'nope' }), compact: await cmd(db, { compact: 't' }), compact_force: await cmd(db, { compact: 't', force: true }), compact_missing: await cmd(db, { compact: 'nope' }), indexes_after: (await t.indexes()).map((i) => i.name), data_after: await t.countDocuments() };
  }, sa);
  C('g09.diag.reindex_compact_replset', async (c, db) => { await db.collection('t').insertOne({ _id: 1 }); return { reindex: await cmd(db, { reIndex: 't' }), reindex_missing: await cmd(db, { reIndex: 'nope' }), compact: await cmd(db, { compact: 't' }), compact_force: await cmd(db, { compact: 't', force: true }), quorum: await cmd(db, { setIndexCommitQuorum: 't', indexNames: ['x'], commitQuorum: 'majority' }) }; }, rs);
  C('g09.diag.set_index_commit_quorum', async (c, db) => { await db.collection('t').insertOne({ _id: 1 }); return { r: await cmd(db, { setIndexCommitQuorum: 't', indexNames: ['x'], commitQuorum: 'majority' }), bad: await cmd(db, { setIndexCommitQuorum: 5, indexNames: ['x'], commitQuorum: 'majority' }) }; }, sa);
  C('g09.diag.filemd5', async (c, db) => {
    const fid = new ObjectId('5f0000000000000000000001'); const { Binary } = require('mongodb');
    await db.collection('fs.chunks').insertMany([{ _id: new ObjectId('5f0000000000000000000010'), files_id: fid, n: 1, data: new Binary(Buffer.from('world')) }, { _id: new ObjectId('5f0000000000000000000011'), files_id: fid, n: 0, data: new Binary(Buffer.from('hello ')) }]);
    await db.collection('fs.chunks').createIndex({ files_id: 1, n: 1 }, { unique: true });
    return { present: await db.command({ filemd5: fid, root: 'fs' }), missing: await db.command({ filemd5: new ObjectId('5f00000000000000000000ff'), root: 'fs' }), other_root: await db.command({ filemd5: fid, root: 'zz' }) };
  }, sa);
  C('g09.diag.fsync', async (c, db) => {
    const A = adm(c); return { plain: (await A.command({ fsync: 1 })).numFiles >= 1, lock: await A.command({ fsync: 1, lock: true }), unlock: await A.command({ fsyncUnlock: 1 }), unlock_again: await cmd(A, { fsyncUnlock: 1 }), other_db: await cmd(db, { fsync: 1 }), logrotate: await A.command({ logRotate: 1 }), logrotate_other_db: await cmd(db, { logRotate: 1 }) };
  }, sa);
  C('g09.diag.aggregate_database_stages', async (c, db) => {
    const agg = (dbname, pipe) => tr(async () => (await c.db(dbname).command({ aggregate: 1, pipeline: pipe, cursor: {} })).cursor.firstBatch);
    const out = {};
    out.current_op = await agg('admin', [{ $currentOp: {} }, { $match: { 'command.aggregate': { $exists: true } } }, { $project: { op: 1, ns: 1, type: 1, active: 1 } }]);
    const keys = await agg('admin', [{ $currentOp: {} }, { $match: { 'command.aggregate': { $exists: true } } }, { $limit: 1 }]); out.current_op_keys = Array.isArray(keys) && keys.length ? Object.keys(keys[0]).sort() : keys;
    const pipes = { not_admin: [{ $currentOp: {} }], bad_opt: [{ $currentOp: { foo: 1 } }], arg_type: [{ $currentOp: 5 }], twice: [{ $currentOp: {} }, { $currentOp: {} }], not_first: [{ $match: {} }, { $currentOp: {} }], empty: [], match: [{ $match: {} }], group: [{ $group: { _id: 1 } }], coll_stats: [{ $collStats: {} }], facet: [{ $facet: { a: [] } }], unknown: [{ $nosuchstage: {} }], search: [{ $search: {} }], list_sessions: [{ $listSessions: {} }], list_cached: [{ $listCachedAndActiveUsers: {} }], local_bad_opt: [{ $listLocalSessions: { foo: 1 } }], local_users: [{ $listLocalSessions: { users: [{ user: 'x', db: 'y' }] } }] };
    for (const [n, p] of Object.entries(pipes)) out[n] = await agg(n === 'not_admin' ? db.databaseName : 'admin', p);
    const sid = (await adm(c).command({ startSession: 1 })).id; await db.command({ insert: 't', documents: [{ _id: 1 }], lsid: sid });
    const sess = await agg('admin', [{ $listLocalSessions: { allUsers: true } }]); const has = (l) => Array.isArray(l) && l.some((s) => s._id.id.toString('hex') === sid.id.toString('hex'));
    out.local_sessions = [has(sess), Array.isArray(sess) && sess.length ? Object.keys(sess[0]).sort() : null, Array.isArray(sess) && sess.length ? Object.keys(sess[0]._id).sort() : null];
    await adm(c).command({ endSessions: [sid] }); out.after_end = !has(await agg('admin', [{ $listLocalSessions: { allUsers: true } }])); out.match_after = await agg('admin', [{ $listLocalSessions: {} }, { $match: { nope: 1 } }]); return out;
  }, sa);
};
