// Item 10 (Node driver): transactionLifetimeLimitSeconds (see g10_txn.py).
module.exports = ({ C }) => {
  const code = async (f) => { try { await f(); return 'ok'; } catch (e) { return [e.code, e.codeName, [...(e.errorLabels || e.errorLabelSet || [])].sort()]; } };
  const sleep = (ms) => new Promise((r) => setTimeout(r, ms));
  C('g10.txn.lifetime_limit', async (c, db) => {
    await db.collection('t').insertOne({ _id: 0 });
    const out = {}; const admin = c.db('admin');
    try {
      out.set = (await admin.command({ setParameter: 1, transactionLifetimeLimitSeconds: 1 })).was;
      let s = c.startSession();
      try {
        s.startTransaction();
        await db.collection('t').insertOne({ _id: 1 }, { session: s });
        await sleep(3500);
        out.insert_after_expiry = await code(() => db.collection('t').insertOne({ _id: 2 }, { session: s }));
        out.commit_after_expiry = await code(() => s.commitTransaction());
      } finally { await s.endSession(); }
      out.docs = (await db.collection('t').find().toArray()).map((d) => d._id).sort();
      s = c.startSession();
      try { s.startTransaction(); await db.collection('t').insertOne({ _id: 3 }, { session: s }); await s.commitTransaction(); } finally { await s.endSession(); }
      out.docs_after = (await db.collection('t').find().toArray()).map((d) => d._id).sort();
    } finally { await admin.command({ setParameter: 1, transactionLifetimeLimitSeconds: 60 }); }
    return out;
  }, { rs: true });
};
