// Item 12 (Node driver): members that send requests and that no earlier test called (see g12_members.py).
module.exports = ({ C, Long, ObjectId, bson, mongodb }) => {
  const { GridFSBucket } = mongodb;
  const tr = async (f) => { try { return await f(); } catch (e) { return e.codeName || e.code ? { err: [e.code, e.codeName] } : { exc: e.constructor.name, msg: String(e.message).slice(0, 80) }; } };
  const seed = async (db, n = 10, name = 't') => { await db.collection(name).insertMany(Array.from({ length: n }, (_, i) => ({ _id: i, a: i, g: i % 3, s: `s${i}` }))); return db.collection(name); };
  const ids = (docs) => docs.map((d) => d._id);
  const sleep = (ms) => new Promise((r) => setTimeout(r, ms));
  const clean = (x) => { if (x && typeof x === 'object' && !Array.isArray(x)) { const o = { ...x }; delete o.$clusterTime; delete o.operationTime; return o; } return x; };

  C('g12.node.create_indexes_and_information', async (c, db) => {
    const t = await seed(db); const out = {};
    out.names = await t.createIndexes([{ key: { a: 1 } }, { key: { g: -1, s: 1 }, name: 'gs' }, { key: { s: 1 }, unique: true, name: 'su', sparse: true }]);
    out.again = await t.createIndexes([{ key: { a: 1 } }]);
    out.indexes = (await t.indexes()).map((i) => { const x = { ...i }; delete x.ns; return x; }).sort((p, q) => (p.name < q.name ? -1 : 1));
    out.indexExists_name = await t.indexExists('gs');
    out.indexExists_names = await t.indexExists(['gs', 'su']);
    out.indexExists_missing = await t.indexExists('zz');
    out.indexExists_some_missing = await t.indexExists(['gs', 'zz']);
    out.info = await t.indexInformation();
    out.info_full = (await t.indexInformation({ full: true })).map((i) => { const x = { ...i }; delete x.ns; return x; }).sort((p, q) => (p.name < q.name ? -1 : 1));
    out.conflict = await tr(() => t.createIndexes([{ key: { a: 1 }, name: 'a_1', unique: true }]));
    out.empty = await tr(() => t.createIndexes([]));
    out.bad_key = await tr(() => t.createIndexes([{ key: { a: 'bogus' } }]));
    out.partial = await t.createIndexes([{ key: { g: 1 }, name: 'part', partialFilterExpression: { a: { $gt: 2 } } }]);
    out.missing_coll_indexes = await tr(() => db.collection('nope').indexes());
    out.missing_coll_exists = await tr(() => db.collection('nope').indexExists('x'));
    out.missing_coll_info = await tr(() => db.collection('nope').indexInformation());
    return out;
  });

  C('g12.node.collection_options', async (c, db) => {
    const out = {};
    await db.createCollection('plain');
    await db.createCollection('cap', { capped: true, size: 4096, max: 50 });
    await db.createCollection('val', { validator: { a: { $gt: 0 } }, validationLevel: 'moderate', validationAction: 'warn' });
    await db.createCollection('col', { collation: { locale: 'fr', strength: 2 } });
    for (const n of ['plain', 'cap', 'val', 'col']) { out[n] = await db.collection(n).options(); out[n + '_isCapped'] = await db.collection(n).isCapped(); }
    out.missing = await tr(() => db.collection('nope').options());
    out.missing_isCapped = await tr(() => db.collection('nope').isCapped());
    return out;
  });

  C('g12.node.cursor_members', async (c, db) => {
    const t = await seed(db); await db.createCollection('cap', { capped: true, size: 4096 }); await db.collection('cap').insertMany([0, 1, 2, 3].map((_id) => ({ _id })));
    const out = {};
    out.flag_tailable_plain = await tr(async () => ids(await t.find().addCursorFlag('tailable', true).toArray()));
    out.flag_tailable_capped = await tr(async () => ids(await db.collection('cap').find().addCursorFlag('tailable', true).limit(2).toArray()));
    out.flag_noCursorTimeout = await tr(async () => ids(await t.find().addCursorFlag('noCursorTimeout', true).limit(2).toArray()));
    out.flag_partial = await tr(async () => ids(await t.find().addCursorFlag('partial', true).limit(2).toArray()));
    out.flag_awaitData = await tr(async () => ids(await db.collection('cap').find().addCursorFlag('tailable', true).addCursorFlag('awaitData', true).maxAwaitTimeMS(100).limit(2).toArray()));
    out.flag_bad = await tr(async () => ids(await t.find().addCursorFlag('bogus', true).toArray()));
    out.maxTimeMS = await tr(async () => ids(await t.find().maxTimeMS(5000).toArray()));
    out.maxTimeMS_agg = await tr(async () => ids(await t.aggregate([{ $sort: { _id: 1 } }]).maxTimeMS(5000).toArray()));
    out.maxTimeMS_bad = await tr(async () => ids(await t.find().maxTimeMS(-1).toArray()));
    out.modifier_comment = await tr(async () => ids(await t.find().addQueryModifier('$comment', 'hi').limit(2).toArray()));
    out.modifier_hint = await tr(async () => ids(await t.find().addQueryModifier('$hint', { _id: 1 }).limit(3).toArray()));
    out.modifier_orderby = await tr(async () => ids(await t.find().addQueryModifier('$orderby', { _id: -1 }).limit(3).toArray()));
    out.modifier_maxScan = await tr(async () => ids(await t.find().addQueryModifier('$maxScan', 5).toArray()));
    out.modifier_maxTimeMS = await tr(async () => ids(await t.find().addQueryModifier('$maxTimeMS', 5000).limit(2).toArray()));
    out.modifier_returnKey = await tr(async () => await t.find().addQueryModifier('$returnKey', true).limit(1).toArray());
    out.modifier_showDiskLoc = await tr(async () => (await t.find().addQueryModifier('$showDiskLoc', true).limit(1).toArray()).map((d) => Object.keys(d).sort()));
    out.modifier_bad = await tr(async () => ids(await t.find().addQueryModifier('bogus', 1).toArray()));
    out.geoNear = await tr(async () => { await db.collection('g').createIndex({ loc: '2dsphere' }); await db.collection('g').insertMany([{ _id: 1, loc: { type: 'Point', coordinates: [0, 0] } }, { _id: 2, loc: { type: 'Point', coordinates: [1, 1] } }]); return (await db.collection('g').aggregate().geoNear({ near: { type: 'Point', coordinates: [0, 0] }, distanceField: 'd', spherical: true }).toArray()).map((d) => [d._id, Math.round(d.d)]); });
    return out;
  });

  C('g12.node.cursor_streams_and_batches', async (c, db) => {
    const t = await seed(db, 25); const out = {};
    const cur = t.find().sort({ _id: 1 }).batchSize(7);
    const got = []; for await (const d of cur) got.push(d._id); out.for_await = got.length;
    const s = t.find().sort({ _id: 1 }).batchSize(7).stream(); const sg = []; for await (const d of s) sg.push(d._id); out.stream = [sg.length, sg[0], sg[24]];
    const cur2 = t.aggregate([{ $sort: { _id: 1 } }], { batchSize: 4 });
    out.hasNext_next = [await cur2.hasNext(), (await cur2.next())._id, (await cur2.tryNext())._id];
    await cur2.close(); out.closed = [cur2.closed, await tr(() => cur2.next())];
    const cur3 = t.find().sort({ _id: 1 }).batchSize(5);
    out.readBufferedDocuments = (await cur3.next(), cur3.readBufferedDocuments(2).map((d) => d._id));
    await cur3.close();
    out.toArray_after_partial = await (async () => { const k = t.find().sort({ _id: 1 }).batchSize(5); await k.next(); return (await k.toArray()).length; })();
    out.clone_rewind = await (async () => { const k = t.find().sort({ _id: 1 }).limit(3); const a = ids(await k.toArray()); k.rewind(); return [a, ids(await k.toArray()), ids(await k.clone().toArray())]; })();
    out.count_like = await t.find({ g: 1 }).count().catch((e) => ({ exc: e.constructor.name }));
    return out;
  });

  C('g12.node.change_stream_members', async (c, db) => {
    const out = {}; await db.createCollection('c');
    for (const [label, w] of [['collection', db.collection('c')], ['database', db], ['cluster', c]]) {
      const cs = w.watch([], { maxAwaitTimeMS: 200 });
      out[label + '_empty_try'] = await cs.tryNext();
      const tok0 = cs.resumeToken; out[label + '_token0'] = tok0 ? Object.keys(tok0) : tok0;
      await db.collection('c').insertOne({ _id: label });
      let ev = null; for (let i = 0; i < 30 && !(ev && ev.ns && ev.ns.db === db.databaseName); i++) ev = await cs.tryNext();
      out[label + '_event'] = ev && { operationType: ev.operationType, fullDocument: ev.fullDocument, documentKey: ev.documentKey };
      out[label + '_token_moved'] = JSON.stringify(cs.resumeToken) !== JSON.stringify(tok0);
      await cs.close(); out[label + '_closed'] = [cs.closed, await tr(() => cs.next())];
      await db.collection('c').deleteMany({});
    }
    const cs = db.collection('c').watch([], { maxAwaitTimeMS: 100 });
    const streamed = (async () => { for await (const e of cs.stream()) return e.operationType; })();
    await sleep(300); await db.collection('c').insertOne({ _id: 'x' });
    out.stream = await streamed; await cs.close();
    const cs2 = db.collection('c').watch([], { maxAwaitTimeMS: 100 });
    await db.collection('c').insertOne({ _id: 'y' });
    out.hasNext_next = [await cs2.hasNext(), (await cs2.next()).operationType]; await cs2.close();
    return out;
  }, { rs: true });

  C('g12.node.session_times', async (c, db) => {
    const out = {}; const s = c.startSession();
    out.before = [s.clusterTime ?? null, s.operationTime ?? null];
    await db.collection('t').insertOne({ _id: 1 }, { session: s });
    out.after_write = [!!s.clusterTime && Object.keys(s.clusterTime).sort(), s.operationTime && s.operationTime._bsontype];
    const ct = s.clusterTime, ot = s.operationTime;
    const s2 = c.startSession();
    s2.advanceClusterTime(ct); s2.advanceOperationTime(ot);
    out.advanced = [JSON.stringify(s2.clusterTime) === JSON.stringify(ct), s2.operationTime.equals(ot)];
    out.causal_read = await db.collection('t').findOne({ _id: 1 }, { session: s2 });
    out.bad_cluster_time = await tr(async () => s2.advanceClusterTime(5));
    out.bad_cluster_time2 = await tr(async () => s2.advanceClusterTime({}));
    out.bad_operation_time = await tr(async () => s2.advanceOperationTime(5));
    await db.collection('t').insertOne({ _id: 2 }, { session: s });
    out.monotonic = s.operationTime.greaterThanOrEqual ? s.operationTime.greaterThanOrEqual(ot) : null;
    await s2.endSession(); await s.endSession();
    return out;
  }, { rs: true });

  C('g12.node.admin_members', async (c, db) => {
    const a = c.db().admin(); const out = {};
    const bi = await a.buildInfo(); out.buildInfo = [bi.version, bi.ok, typeof bi.gitVersion, Array.isArray(bi.versionArray)];
    const si = await a.serverInfo(); out.serverInfo = [si.version, si.ok];
    out.ping = clean(await a.ping());
    out.command = clean(await a.command({ ping: 1 }));
    out.command_bad = await tr(() => a.command({ nosuchcommand: 1 }));
    const ld = await a.listDatabases(); out.listDatabases = [Object.keys(ld).sort().filter((k) => !['$clusterTime', 'operationTime'].includes(k)), ld.databases.some((d) => d.name === 'admin')];
    out.listDatabases_nameOnly = (await a.listDatabases({ nameOnly: true })).databases.every((d) => Object.keys(d).join() === 'name');
    out.listDatabases_filter = (await a.listDatabases({ filter: { name: 'admin' }, nameOnly: true })).databases.map((d) => d.name);
    const ss = await a.serverStatus(); out.serverStatus = [ss.ok, ss.version, typeof ss.uptime, typeof ss.connections.current];
    await db.collection('v').insertMany([{ _id: 1 }, { _id: 2 }]);
    const vc = await a.validateCollection(db.collection('v').collectionName, { dbName: db.databaseName }).catch((e) => ({ err: [e.code, e.codeName] }));
    out.validateCollection = vc && vc.err ? vc : [vc.valid, vc.nrecords, vc.ok];
    out.validateCollection_missing = await tr(() => a.validateCollection('nope', { dbName: db.databaseName }));
    out.replSetGetStatus = await tr(async () => { const r = await a.replSetGetStatus(); return [r.ok, r.set, r.myState, r.members.length]; });
    return out;
  }, { rs: true });

  C('g12.node.admin_members_standalone', async (c, db) => {
    const a = c.db().admin(); const out = {};
    out.ping = clean(await a.ping());
    out.replSetGetStatus = await tr(() => a.replSetGetStatus());
    out.removeUser_missing = await tr(() => a.removeUser('nobody'));
    out.db_removeUser_missing = await tr(() => db.removeUser('nobody'));
    return out;
  }, { sa: true });

  C('g12.node.gridfs_read_stream_members', async (c, db) => {
    const b = new GridFSBucket(db, { chunkSizeBytes: 4 }); const out = {};
    const data = Buffer.from('0123456789abcdefghij');
    await new Promise((res, rej) => { const w = b.openUploadStream('f.txt'); w.on('finish', res).on('error', rej); w.end(data); });
    const read = (s) => new Promise((res, rej) => { const parts = []; s.on('data', (d) => parts.push(d)).on('end', () => res(Buffer.concat(parts).toString())).on('error', rej); });
    out.whole = await read(b.openDownloadStreamByName('f.txt'));
    out.start = await read(b.openDownloadStreamByName('f.txt').start(6));
    out.end = await read(b.openDownloadStreamByName('f.txt').end(6));
    out.start_end = await read(b.openDownloadStreamByName('f.txt').start(5).end(13));
    out.start_end_opts = await read(b.openDownloadStreamByName('f.txt', { start: 3, end: 9 }));
    out.start_after_init = await tr(async () => { const s = b.openDownloadStreamByName('f.txt'); await read(s); return s.start(1); });
    out.end_after_init = await tr(async () => { const s = b.openDownloadStreamByName('f.txt'); await read(s); return s.end(1); });
    out.start_beyond = await tr(() => read(b.openDownloadStreamByName('f.txt').start(100)));
    out.abort = await (async () => { const s = b.openDownloadStreamByName('f.txt'); const parts = []; await new Promise((res) => { s.once('data', (d) => { parts.push(d.toString()); s.abort().then(res); }); }); await sleep(100); return [parts.length, s.destroyed]; })();
    out.abort_before_read = await tr(async () => { const s = b.openDownloadStreamByName('f.txt'); await s.abort(); return s.destroyed; });
    out.missing = await tr(() => read(b.openDownloadStreamByName('nope.txt')));
    out.revision = await tr(() => read(b.openDownloadStreamByName('f.txt', { revision: 3 })));
    return out;
  });

  const normEv = (ev) => { const rec = (x) => { if (Array.isArray(x)) return x.map(rec); if (x && typeof x === 'object' && !x._bsontype && !(x instanceof Date) && !Buffer.isBuffer(x)) { const o = {}; for (const [k, v] of Object.entries(x)) { if (['_id', 'clusterTime', 'wallTime', 'lsid', 'txnNumber'].includes(k)) continue; o[k] = (k === 'uuid' || k === 'collectionUUID') ? '<uuid>' : rec(v); } return o; } return x; }; return rec(ev); };
  C('g12.node.change_stream_expanded_ddl_events', async (c, db) => {
    const out = {}; const cs = db.watch([], { showExpandedEvents: true, maxAwaitTimeMS: 200 });
    const coll = await db.createCollection('c');
    await coll.createIndex({ a: 1 }, { name: 'a_1' }); await coll.createIndex({ b: 1 }, { name: 'b_1', unique: true });
    await db.command({ collMod: 'c', index: { name: 'a_1', hidden: true } });
    await db.command({ collMod: 'c', validator: { x: { $exists: true } }, validationLevel: 'moderate' });
    await db.command({ collMod: 'c', index: { keyPattern: { b: 1 }, prepareUnique: true } });
    await coll.dropIndex('a_1'); await db.command({ dropIndexes: 'c', index: ['b_1'] });
    await coll.createIndex({ z: 1 }, { name: 'z_1' }); await db.command({ dropIndexes: 'c', index: '*' });
    await c.db('admin').command({ renameCollection: `${db.databaseName}.c`, to: `${db.databaseName}.d` });
    await db.dropCollection('d');
    const evs = []; for (let i = 0; i < 40; i++) { const e = await cs.tryNext(); if (e === null) { if (evs.length >= 11) break; continue; } evs.push(normEv(e)); }
    out.events = evs; out.types = evs.map((e) => e.operationType); await cs.close(); return out;
  }, { rs: true });
};
