// Driver Node.js: connessione, CRUD, aggregazione, transazione. Uso: node prova.js <uri>
const { MongoClient } = require('mongodb');
const uri = process.argv[2] || 'mongodb://localhost:27018/?replicaSet=rs0';
const out = { driver: 'node mongodb ' + require('mongodb/package.json').version, steps: {} };
async function step(name, fn) {
  try { await Promise.race([fn(), new Promise((_, rej) => setTimeout(() => rej(new Error('nessuna risposta in 15 s')), 15000))]); out.steps[name] = 'ok'; } catch (e) { out.steps[name] = 'ERRORE: ' + String(e.message).slice(0, 200); }
}
function eq(a, b, what) { if (JSON.stringify(a) !== JSON.stringify(b)) throw new Error(`${what}: atteso ${JSON.stringify(b)}, avuto ${JSON.stringify(a)}`); }
(async () => {
  const client = new MongoClient(uri, { serverSelectionTimeoutMS: 5000 });
  const db = client.db('drv_node'); const c = db.collection('t');
  await step('connessione', async () => { await client.connect(); eq((await db.command({ ping: 1 })).ok, 1, 'ping'); });
  await step('crud', async () => {
    await c.deleteMany({});
    await c.insertMany([{ _id: 1, n: 'a', v: 1 }, { _id: 2, n: 'b', v: 2 }, { _id: 3, n: 'c', v: 3 }]);
    eq((await c.findOne({ _id: 2 })).n, 'b', 'findOne');
    eq((await c.updateOne({ _id: 1 }, { $inc: { v: 10 } })).modifiedCount, 1, 'updateOne');
    eq((await c.findOneAndUpdate({ _id: 3 }, { $set: { n: 'z' } }, { returnDocument: 'after' })).n, 'z', 'findOneAndUpdate');
    eq((await c.deleteOne({ _id: 2 })).deletedCount, 1, 'deleteOne');
    eq(await c.countDocuments(), 2, 'countDocuments');
  });
  await step('aggregazione', async () => {
    const r = await c.aggregate([{ $group: { _id: null, s: { $sum: '$v' } } }]).toArray();
    eq(r[0].s, 14, 'sum');
  });
  await step('transazione', async () => {
    const s = client.startSession();
    try {
      await s.withTransaction(async () => {
        await c.insertOne({ _id: 10 }, { session: s });
        await c.updateOne({ _id: 1 }, { $set: { tx: true } }, { session: s });
      });
    } finally { await s.endSession(); }
    eq(await c.countDocuments({ $or: [{ _id: 10 }, { tx: true }] }), 2, 'dopo commit');
  });
  await step('indice e change stream', async () => {
    await c.createIndex({ n: 1 }, { unique: true });
    const cs = c.watch();
    const pending = cs.next(); // the driver opens the stream on the first next()
    await new Promise(r => setTimeout(r, 500));
    await c.insertOne({ _id: 20, n: 'w' });
    const ev = await pending; eq(ev.operationType, 'insert', 'evento'); await cs.close();
  });
  console.log(JSON.stringify(out));
  await client.close(true); process.exit(0);
})();
