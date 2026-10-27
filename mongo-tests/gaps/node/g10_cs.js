// Item 10 (Node driver): change streams beyond the basics (see g10_cs.py).
module.exports = ({ C }) => {
  const sleep = (ms) => new Promise((r) => setTimeout(r, ms));
  const norm = (x) => { if (Array.isArray(x)) return x.map(norm); if (x && x._bsontype === 'Binary' && x.sub_type === 4) return '<uuid>'; if (x && typeof x === 'object' && !x._bsontype && !(x instanceof Date)) return Object.fromEntries(Object.entries(x).map(([k, v]) => [k, norm(v)])); if (typeof x === 'string') return x.replace(/Timestamp\(\d+, \d+\)/g, 'Timestamp(T)'); return x; };
  const strip = (e) => { const o = { ...e }; const hadUuid = 'collectionUUID' in o; for (const k of ['_id', 'clusterTime', 'wallTime', 'lsid', 'txnNumber', 'collectionUUID']) delete o[k]; return norm({ __keys: Object.keys(o), __uuid: hadUuid, ...o }); };
  const run = async (db, c, opts, actions, target = 'coll', pre) => {
    if (pre) await pre(db);
    const t = target === 'db' ? db : db.collection('c');
    let st; try { st = t.watch([], { maxAwaitTimeMS: 200, ...opts }); await st.tryNext(); } catch (e) { return { open_err: [e.code, e.codeName, norm(e.errmsg || e.message)] }; }
    const out = [];
    try {
      await actions(db, c);
      for (let i = 0; i < 20; i++) { let e; try { e = await st.tryNext(); } catch (ex) { out.push({ err: [ex.code, ex.codeName, norm(ex.errmsg || ex.message)] }); break; } if (e) out.push(strip(e)); }
    } finally { await st.close(); }
    return out;
  };
  const crud = async (db) => { const c = db.collection('c'); await c.insertOne({ _id: 1, a: 1 }); await c.updateOne({ _id: 1 }, { $set: { a: 2 } }); await c.replaceOne({ _id: 1 }, { a: 3 }); await c.deleteOne({ _id: 1 }); await c.deleteOne({ _id: 77 }); await c.updateOne({ _id: 5 }, { $set: { x: 1 } }, { upsert: true }); await c.updateOne({ _id: 5 }, { $set: { x: 1 } }); };
  const images = (db) => db.createCollection('c', { changeStreamPreAndPostImages: { enabled: true } });
  const OPTS = { default: {}, update_lookup: { fullDocument: 'updateLookup' }, when_available: { fullDocument: 'whenAvailable' }, required: { fullDocument: 'required' }, before_when: { fullDocumentBeforeChange: 'whenAvailable' }, before_required: { fullDocumentBeforeChange: 'required' }, both: { fullDocument: 'whenAvailable', fullDocumentBeforeChange: 'whenAvailable' }, bad_full: { fullDocument: 'nope' }, expanded: { showExpandedEvents: true } };
  for (const [n, o] of Object.entries(OPTS)) {
    C(`g10.cs.opts_${n}_plain`, (c, db) => run(db, c, o, crud), { rs: true });
    C(`g10.cs.opts_${n}_images`, (c, db) => run(db, c, o, crud, 'coll', images), { rs: true });
  }
  const ddl = async (db) => {
    await db.createCollection('e1'); await db.collection('e1').createIndexes([{ key: { a: 1 }, name: 'a_1', unique: true }, { key: { b: 1 }, name: 'b_1' }]);
    await db.command({ collMod: 'e1', index: { name: 'a_1', hidden: true } }); await db.command({ collMod: 'e1', validator: { a: { $gt: 0 } } }); await db.collection('e1').dropIndex('a_1');
    await db.collection('e2').insertOne({ _id: 1 }); await db.collection('e1').rename('e3'); await db.collection('e2').rename('e3', { dropTarget: true }); await db.collection('e3').drop();
  };
  C('g10.cs.ddl_expanded', (c, db) => run(db, c, { showExpandedEvents: true }, ddl, 'db'), { rs: true });
  C('g10.cs.ddl_standard', (c, db) => run(db, c, {}, ddl, 'db'), { rs: true });
  C('g10.cs.rename_collection_stream', (c, db) => run(db, c, {}, async (d) => { await d.collection('c').insertOne({ _id: 1 }); await d.collection('c').rename('c2'); await d.collection('c2').insertOne({ _id: 2 }); }), { rs: true });
  C('g10.cs.drop_database_db_stream', (c, db) => run(db, c, {}, async (d) => { await d.collection('z').insertOne({ _id: 1 }); await d.dropDatabase(); }, 'db'), { rs: true });
};
