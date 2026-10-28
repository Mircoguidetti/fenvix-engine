// Item 9 (Node driver): capped collections, collMod / convertToCapped / cloneCollectionAsCapped, tailable cursors, dataSize.
module.exports = ({ C }) => {
  const clean = (m) => (m || '').replace(/UUID\("[0-9a-f-]+"\)/g, 'UUID(<u>)');
  const tr = async (f) => { try { return await f(); } catch (e) { return { err: [e.code, e.codeName, clean(e.errmsg || e.message)] }; } };
  const opts = async (db, n) => ((await db.listCollections({ name: n }).toArray())[0] || {}).options;
  const ids = async (db, n, find = {}, o = {}) => (await db.collection(n).find(find, o).toArray()).map((d) => d._id);
  const cmd = (db, c) => tr(() => db.command(c));
  const strip = (r, keys) => (r.err ? r : Object.fromEntries(Object.entries(r).filter(([k]) => !keys.includes(k))));

  C('g09.capped.create_options', async (c, db) => {
    const out = {};
    const specs = { size: { capped: true, size: 4096 }, max: { capped: true, size: 100000, max: 5 }, tiny: { capped: true, size: 100 }, nocap_size: { size: 1000 }, no_size: { capped: true }, size0: { capped: true, size: 0 }, neg: { capped: true, size: -5 },
      big: { capped: true, size: 2 ** 60 }, maxbig: { capped: true, size: 4096, max: 2 ** 40 }, capped_str: { capped: 'x', size: 5000 }, size_str: { capped: true, size: 'x' }, max_nocap: { max: 5 }, max0: { capped: true, size: 4096, max: 0 }, max_neg: { capped: true, size: 4096, max: -1 } };
    for (const [n, s] of Object.entries(specs)) out[n] = [await cmd(db, { create: 'c_' + n, ...s }), await opts(db, 'c_' + n)];
    out.recreate_same = await cmd(db, { create: 'c_size', capped: true, size: 4096 }); out.recreate_other = await cmd(db, { create: 'c_size', capped: true, size: 8192 });
    return out;
  });
  C('g09.capped.eviction', async (c, db) => {
    await db.command({ create: 'a', capped: true, size: 4096 }); for (let i = 0; i < 200; i++) await db.collection('a').insertOne({ _id: i, p: 'y'.repeat(30) });
    const got = await ids(db, 'a'); const st = await db.command({ collStats: 'a' });
    await db.command({ create: 'b', capped: true, size: 100000, max: 5 }); for (let i = 0; i < 30; i++) await db.collection('b').insertOne({ _id: i });
    await db.command({ create: 'm', capped: true, size: 2048, max: 20 }); await db.collection('m').insertMany(Array.from({ length: 100 }, (_, i) => ({ _id: i, p: 'z'.repeat(50) })));
    return { n: got.length, first: got.slice(0, 3), last: got.slice(-3), stats: { capped: st.capped, size: st.size, count: st.count, max: st.max, maxSize: st.maxSize }, byMax: await ids(db, 'b'), batch: await ids(db, 'm'), rev: (await ids(db, 'm', {}, { sort: { $natural: -1 } })).slice(0, 3) };
  });
  C('g09.capped.single_doc_over_cap', async (c, db) => { await db.command({ create: 'c', capped: true, size: 1000 }); await db.collection('c').insertOne({ _id: 1 }); const r = await tr(async () => (await db.collection('c').insertOne({ _id: 'big', p: 'b'.repeat(5000) })).acknowledged); return { r, ids: await ids(db, 'c') }; });
  C('g09.capped.update_delete', async (c, db) => {
    await db.command({ create: 'c', capped: true, size: 4096 }); for (let i = 0; i < 10; i++) await db.collection('c').insertOne({ _id: i, p: 'y'.repeat(30) });
    const t = db.collection('c'); const out = {};
    out.same_size = await tr(async () => (await t.updateOne({ _id: 3 }, { $set: { p: 'z'.repeat(30) } })).modifiedCount); out.bigger = await tr(async () => (await t.updateOne({ _id: 3 }, { $set: { p: 'z'.repeat(31) } })).modifiedCount);
    out.smaller = await tr(async () => (await t.updateOne({ _id: 4 }, { $set: { p: 'z'.repeat(5) } })).modifiedCount); out.delete_one = await tr(async () => (await t.deleteOne({ _id: 5 })).deletedCount);
    out.fam_remove = await tr(async () => (await db.command({ findAndModify: 'c', query: { _id: 7 }, remove: true })).value); out.delete_many = await tr(async () => (await t.deleteMany({ _id: { $gte: 8 } })).deletedCount);
    out.ids = await ids(db, 'c'); const st = await db.command({ collStats: 'c' }); out.stats = { size: st.size, count: st.count }; return out;
  });
  C('g09.capped.indexes', async (c, db) => {
    await db.command({ create: 'c', capped: true, size: 4096 }); const t = db.collection('c');
    const out = { secondary: await tr(() => t.createIndex('p')), ttl: await tr(() => t.createIndex('q', { expireAfterSeconds: 10 })), unique: await tr(() => t.createIndex('u', { unique: true })), drop_all: await cmd(db, { dropIndexes: 'c', index: '*' }) };
    for (let i = 0; i < 150; i++) await t.insertOne({ _id: i, p: i % 5, u: i });
    out.n = await t.countDocuments(); out.dup = await tr(() => t.insertOne({ _id: 149 })); return out;
  });
  C('g09.capped.drop_rename', async (c, db) => {
    await db.command({ create: 'c', capped: true, size: 1000, max: 3 }); for (let i = 0; i < 10; i++) await db.collection('c').insertOne({ _id: i });
    const out = { rename: await tr(async () => (await db.collection('c').rename('d')).collectionName), opts: await opts(db, 'd') }; for (let i = 10; i < 14; i++) await db.collection('d').insertOne({ _id: i }); out.ids = await ids(db, 'd'); return out;
  });
  C('g09.capped.transactions', async (c, db) => {
    await db.command({ create: 'cap', capped: true, size: 4096 }); await db.collection('cap').insertOne({ _id: 1 }); const out = {};
    const run = async (name, f) => { const s = c.startSession(); try { s.startTransaction(); out[name] = await tr(() => f(s)); } finally { try { await s.abortTransaction(); } catch (e) { /* ignore */ } await s.endSession(); } };
    const t = db.collection('cap');
    await run('insert', async (s) => (await t.insertOne({ _id: 2 }, { session: s })).acknowledged); await run('read', (s) => t.countDocuments({}, { session: s }));
    await run('update', async (s) => (await t.updateOne({ _id: 1 }, { $set: { a: 1 } }, { session: s })).modifiedCount); await run('delete', async (s) => (await t.deleteOne({ _id: 1 }, { session: s })).deletedCount);
    out.ids = await ids(db, 'cap'); return out;
  }, { rs: true });
  C('g09.capped.collmod_convert_clone', async (c, db) => {
    await db.command({ create: 'cap', capped: true, size: 4096 }); for (let i = 0; i < 10; i++) await db.collection('cap').insertOne({ _id: i }); await db.collection('nc').insertOne({ _id: 1 });
    const out = {};
    for (const [n, cm] of Object.entries({ noncapped: { collMod: 'nc', cappedSize: 8192 }, size0: { collMod: 'cap', cappedSize: 0 }, size_str: { collMod: 'cap', cappedSize: 'x' }, max_neg: { collMod: 'cap', cappedMax: -1 }, max0: { collMod: 'cap', cappedMax: 0 }, both: { collMod: 'cap', cappedSize: 10000, cappedMax: 5 }, max3: { collMod: 'cap', cappedMax: 3 } })) out[n] = [await cmd(db, cm), await opts(db, 'cap'), await db.collection('cap').countDocuments()];
    await db.collection('cap').insertOne({ _id: 100 }); out.after_insert = await ids(db, 'cap');
    await db.collection('big').insertMany(Array.from({ length: 10 }, (_, i) => ({ _id: i, p: 'x'.repeat(20) }))); await db.collection('big').createIndex('p', { name: 'p_1' });
    out.convert_missing = await cmd(db, { convertToCapped: 'nope', size: 8192 }); out.convert_no_size = await cmd(db, { convertToCapped: 'big' });
    out.convert = [await cmd(db, { convertToCapped: 'big', size: 100 }), await opts(db, 'big'), await ids(db, 'big'), (await db.collection('big').indexes()).map((i) => i.name)];
    await db.collection('nc2').insertMany([0, 1, 2, 3, 4].map((i) => ({ _id: i, p: 'x'.repeat(20) })));
    out.clone = [await cmd(db, { cloneCollectionAsCapped: 'nc2', toCollection: 'cl', size: 5000 }), await opts(db, 'cl'), await ids(db, 'cl')];
    out.clone_existing = await cmd(db, { cloneCollectionAsCapped: 'nc2', toCollection: 'cl', size: 5000 }); out.clone_missing = await cmd(db, { cloneCollectionAsCapped: 'nope', toCollection: 'cl2', size: 5000 });
    out.clone_small = [await cmd(db, { cloneCollectionAsCapped: 'nc2', toCollection: 'cl4', size: 100 }), await ids(db, 'cl4')]; return out;
  });
  C('g09.tailable.find_getmore', async (c, db) => {
    await db.command({ create: 'cap', capped: true, size: 4096 }); for (let i = 0; i < 5; i++) await db.collection('cap').insertOne({ _id: 10 + i, v: i });
    const r = await db.command({ find: 'cap', filter: {}, tailable: true, batchSize: 2 }); const cid = r.cursor.id; const out = { first: r.cursor.firstBatch.map((d) => d._id), open: cid.toString() !== '0' };
    out.seq = []; for (let i = 0; i < 5; i++) { const g = await db.command({ getMore: cid, collection: 'cap', batchSize: 2 }); out.seq.push([g.cursor.id.toString() === cid.toString(), g.cursor.nextBatch.map((d) => d._id)]); }
    await db.collection('cap').insertOne({ _id: 99 }); const g = await db.command({ getMore: cid, collection: 'cap', batchSize: 2 }); out.after_insert = [g.cursor.id.toString() === cid.toString(), g.cursor.nextBatch.map((d) => d._id)];
    const f = (await db.command({ find: 'cap', filter: { v: { $gt: 2 } }, tailable: true })).cursor; out.filter = [f.firstBatch.map((d) => d._id), f.id.toString() !== '0'];
    out.kill = (await db.command({ killCursors: 'cap', cursors: [cid] })).cursorsKilled.map(String).join() === cid.toString();
    // the driver's tailable cursor
    const cur = db.collection('cap').find({}, { tailable: true, batchSize: 2 }); const got = []; for (let i = 0; i < 6; i++) { const d = await cur.tryNext(); if (d) got.push(d._id); } out.driver = got; await cur.close(); return out;
  });
  C('g09.tailable.await_data', async (c, db) => {
    await db.command({ create: 'cap', capped: true, size: 4096 }); await db.collection('cap').insertOne({ _id: 1 });
    const r = await db.command({ find: 'cap', filter: {}, tailable: true, awaitData: true, batchSize: 100 }); const cid = r.cursor.id; const t = Date.now();
    const g = await db.command({ getMore: cid, collection: 'cap', maxTimeMS: 500 }); const el = Date.now() - t;
    return { first: r.cursor.firstBatch.length, empty: g.cursor.nextBatch, same: g.cursor.id.toString() === cid.toString(), waited: el > 350 && el < 1500 };
  });
  C('g09.tailable.errors', async (c, db) => {
    await db.command({ create: 'cap', capped: true, size: 4096 }); await db.collection('cap').insertOne({ _id: 1, v: 1 }); await db.collection('nc').insertOne({ _id: 1 }); const out = {};
    for (const [n, cm] of Object.entries({ sort: { find: 'cap', tailable: true, sort: { v: 1 } }, natural_sort: { find: 'cap', tailable: true, sort: { $natural: 1 } }, limit: { find: 'cap', tailable: true, limit: 3 }, single: { find: 'cap', tailable: true, singleBatch: true }, await_alone: { find: 'cap', awaitData: true }, non_capped: { find: 'nc', tailable: true }, skip: { find: 'cap', tailable: true, skip: 2 }, projection: { find: 'cap', tailable: true, projection: { v: 1 } } })) {
      const r = await cmd(db, cm); out[n] = r.err ? r : { ok: true, id: r.cursor.id.toString() !== '0', n: r.cursor.firstBatch.length };
    }
    return out;
  });
  C('g09.tailable.lost_position', async (c, db) => {
    await db.command({ create: 'cap2', capped: true, size: 4096 }); for (let i = 0; i < 10; i++) await db.collection('cap2').insertOne({ _id: i, p: 'x'.repeat(200) });
    const r = await db.command({ find: 'cap2', tailable: true, batchSize: 2 }); for (let i = 0; i < 60; i++) await db.collection('cap2').insertOne({ _id: 100 + i, p: 'y'.repeat(200) });
    return cmd(db, { getMore: r.cursor.id, collection: 'cap2', batchSize: 2 });
  });
  C('g09.datasize', async (c, db) => {
    await db.collection('nc2').insertMany([0, 1, 2, 3, 4].map((i) => ({ _id: i }))); const out = {};
    for (const [n, cm] of Object.entries({ plain: { dataSize: `${db.databaseName}.nc2` }, missing: { dataSize: `${db.databaseName}.nope` }, nodot: { dataSize: 'nodot' }, range: { dataSize: `${db.databaseName}.nc2`, keyPattern: { _id: 1 }, min: { _id: 1 }, max: { _id: 3 } }, estimate: { dataSize: `${db.databaseName}.nc2`, estimate: true } })) out[n] = strip(await cmd(db, cm), ['millis']);
    out.other_db = strip(await cmd(c.db('admin'), { dataSize: `${db.databaseName}.nc2` }), ['millis']); return out;
  });
};
