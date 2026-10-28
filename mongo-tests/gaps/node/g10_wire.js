// Item 10 (Node driver): OP_COMPRESSED (zlib) and the hello reply (see g10_wire.py).
module.exports = ({ C, MongoClient }) => {
  C('g10.wire.compression_zlib', async (client, db) => {
    const c = await new MongoClient(client.__uri, { compressors: ['zlib'], directConnection: true }).connect();
    try {
      const h = await c.db('admin').command({ hello: 1, compression: ['zlib', 'bogus'] });
      const t = c.db(db.databaseName).collection('t');
      await t.insertMany(Array.from({ length: 50 }, (_, i) => ({ _id: i, s: 'x'.repeat(200) })));
      return { negotiated: h.compression, docs: await t.find({}, { projection: { _id: 1 } }).sort({ _id: 1 }).limit(5).toArray(), count: await t.countDocuments({}), agg: await t.aggregate([{ $group: { _id: null, n: { $sum: 1 } } }]).toArray() };
    } finally { await c.close(); }
  }, { sa: true });
};
