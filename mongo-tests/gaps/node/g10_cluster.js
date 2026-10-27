// Item 10 (Node driver): cluster time, sessions and readConcern.afterClusterTime (see g10_cluster.py).
module.exports = ({ C, bson }) => {
  const { Timestamp } = bson;
  const tr = async (f) => { try { return await f(); } catch (e) { return { err: [e.code, e.codeName, String(e.errmsg || e.message || '').replace(/Timestamp\(\d+, \d+\)/g, 'Timestamp(T)')] }; } };
  const adm = (c) => c.db('admin');
  const fields = (r) => {
    const ct = r.$clusterTime;
    const out = { has_clusterTime: ct !== undefined, has_operationTime: 'operationTime' in r };
    if (ct !== undefined) {
      const sig = ct.signature || {};
      out.ct_keys = Object.keys(ct).sort(); out.sig_keys = Object.keys(sig).sort(); out.hash_len = sig.hash ? sig.hash.length() : 0; out.keyId = Number(sig.keyId === undefined ? -1 : sig.keyId);
      out.ct_type = ct.clusterTime && ct.clusterTime._bsontype === 'Timestamp' ? 'Timestamp' : typeof ct.clusterTime;
      out.op_type = r.operationTime && r.operationTime._bsontype === 'Timestamp' ? 'Timestamp' : typeof r.operationTime;
      out.op_le_ct = r.operationTime !== undefined && r.operationTime.compare(ct.clusterTime) <= 0;
    }
    return out;
  };
  const errReply = async (db) => { try { await db.command({ find: 't', filter: { $bogus: 1 } }); } catch (e) { return e.result || e; } return {}; };
  const replyFields = async (c, db) => {
    await db.collection('t').insertOne({ _id: 1 });
    return { ping: fields(await db.command({ ping: 1 })), insert: fields(await db.command({ insert: 't', documents: [{ _id: 2 }] })), find: fields(await db.command({ find: 't', filter: {} })), error: fields(await errReply(db)), hello: fields(await adm(c).command({ hello: 1 })) };
  };
  C('g10.cluster.reply_fields', replyFields, { rs: true });
  C('g10.cluster.reply_fields_sa', replyFields, { sa: true });
  C('g10.cluster.session_times', async (c, db) => {
    const out = {};
    const s = c.startSession({ causalConsistency: true });
    try {
      out.before = [s.operationTime === undefined, s.clusterTime === undefined];
      await db.collection('t').insertOne({ _id: 1 }, { session: s });
      const w = s.operationTime;
      out.after_write = [w !== undefined, s.clusterTime !== undefined, w && w._bsontype === 'Timestamp' ? 'Timestamp' : typeof w];
      await db.collection('t').insertOne({ _id: 2 }, { session: s });
      const w2 = s.operationTime;
      out.write_advances = w2.compare(w) > 0;
      out.read_own_writes = (await db.collection('t').find({}, { session: s }).sort({ _id: 1 }).toArray()).map((d) => d._id);
      out.read_time = s.operationTime.compare(w2) >= 0;
      out.cluster_ge_op = s.clusterTime.clusterTime.compare(s.operationTime) >= 0;
      const s2 = c.startSession({ causalConsistency: true });
      try {
        s2.advanceClusterTime(s.clusterTime); s2.advanceOperationTime(s.operationTime);
        out.other_session_reads = await db.collection('t').countDocuments({}, { session: s2 });
      } finally { await s2.endSession(); }
    } finally { await s.endSession(); }
    return out;
  }, { rs: true });
  const rcTable = async (c, db, rs) => {
    await db.collection('t').insertOne({ _id: 1 });
    let ts;
    if (rs) { const s = c.startSession(); try { await db.collection('t').insertOne({ _id: 2 }, { session: s }); ts = s.operationTime; } finally { await s.endSession(); } } else ts = new Timestamp({ t: 1700000000, i: 1 });
    const T = (t, i) => new Timestamp({ t, i });
    const cases = {
      none: null, level_local: { level: 'local' }, level_majority: { level: 'majority' }, level_available: { level: 'available' }, level_snapshot: { level: 'snapshot' }, level_linearizable: { level: 'linearizable' },
      level_bad: { level: 'nope' }, level_int: { level: 5 }, unknown: { bogus: 1 }, act: { afterClusterTime: ts }, act_local: { afterClusterTime: ts, level: 'local' },
      act_majority: { afterClusterTime: ts, level: 'majority' }, act_available: { afterClusterTime: ts, level: 'available' }, act_snapshot: { afterClusterTime: ts, level: 'snapshot' },
      act_linearizable: { afterClusterTime: ts, level: 'linearizable' }, act_zero: { afterClusterTime: T(0, 0) }, act_int: { afterClusterTime: 5 },
      act_past: { afterClusterTime: T(ts.t - 10, 1) }, act_future: { afterClusterTime: T(ts.t + 100000, 0), maxTimeMS: 300 },
      act_and_aot: { afterClusterTime: ts, afterOpTime: { ts, t: 1 } }, empty: {},
    };
    const out = {};
    for (const [name, rc0] of Object.entries(cases)) {
      for (const [cn, cmd] of [['find', { find: 't', filter: { _id: 1 } }], ['count', { count: 't' }], ['aggregate', { aggregate: 't', pipeline: [], cursor: {} }], ['distinct', { distinct: 't', key: '_id' }]]) {
        const cc = { ...cmd };
        if (rc0 !== null) { const rc = { ...rc0 }; const mt = rc.maxTimeMS; delete rc.maxTimeMS; cc.readConcern = rc; if (mt) cc.maxTimeMS = mt; }
        const r = await tr(() => db.command(cc));
        if (r.err) out[`${name}.${cn}`] = r;
        else { const cur = r.cursor; out[`${name}.${cn}`] = { ok: true, n: r.n === undefined ? null : r.n, values: r.values === undefined ? null : r.values, docs: cur ? cur.firstBatch.length : null, atClusterTime: !!(cur && 'atClusterTime' in cur) }; }
      }
    }
    return out;
  };
  C('g10.cluster.read_concern_rs', (c, db) => rcTable(c, db, true), { rs: true });
  C('g10.cluster.read_concern_sa', (c, db) => rcTable(c, db, false), { sa: true });
};
