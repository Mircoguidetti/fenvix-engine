const { MongoClient } = require('mongodb');
(async () => {
  for (const port of [27017, 27018]) {
    const c = new MongoClient(`mongodb://localhost:${port}`, { serverSelectionTimeoutMS: 5000 });
    await c.connect();
    const db = c.db('probe_node');
    await db.collection('t').insertOne({ _id: 1, a: 1 });
    console.log(port, await db.collection('t').find().toArray(), (await db.listCollections().toArray()).map(x => x.name));
    console.log(await db.collection('t').countDocuments({}), await db.collection('t').estimatedDocumentCount());
    await db.dropDatabase();
    await c.close();
  }
})().catch(e => { console.error('ERR', e); process.exit(1); });
