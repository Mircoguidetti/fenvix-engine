// mongosh: comandi tipici dello sviluppatore. Uso: mongosh <uri> --quiet --file mongosh_prova.js
const out = {};
function step(n, f) { try { f(); out[n] = 'ok'; } catch (e) { out[n] = 'ERRORE: ' + String(e.message).slice(0, 200); } }
const d = db.getSiblingDB('msh');
step('show dbs / show collections', () => { db.adminCommand({ listDatabases: 1 }); d.getCollectionNames(); });
step('insert/find/update/delete', () => {
  d.t.drop(); d.t.insertMany([{ _id: 1, a: 1 }, { _id: 2, a: 2 }]);
  if (d.t.find({ a: { $gt: 1 } }).toArray().length !== 1) throw new Error('find');
  d.t.updateOne({ _id: 1 }, { $set: { b: 1 } }); d.t.deleteOne({ _id: 2 });
  if (d.t.countDocuments() !== 1) throw new Error('count');
});
step('aggregate', () => { const r = d.t.aggregate([{ $group: { _id: null, n: { $sum: 1 } } }]).toArray(); if (r[0].n !== 1) throw new Error('agg'); });
step('createIndex / getIndexes', () => { d.t.createIndex({ a: 1 }); if (d.t.getIndexes().length !== 2) throw new Error('idx'); });
step('explain', () => { d.t.find({ a: 1 }).explain(); });
step('stats', () => { d.stats(); d.t.stats(); });
step('serverStatus / db.version()', () => { db.serverStatus(); out.version = db.version(); });
step('rs.status()', () => { rs.status(); });
step('transazione', () => {
  const s = db.getMongo().startSession(); s.startTransaction();
  s.getDatabase('msh').t.insertOne({ _id: 9 }); s.commitTransaction(); s.endSession();
  if (d.t.countDocuments({ _id: 9 }) !== 1) throw new Error('tx');
});
step('createUser / getUsers', () => { try { d.dropUser('u'); } catch (e) {} d.createUser({ user: 'u', pwd: 'p-123456', roles: ['read'] }); d.getUsers(); d.dropUser('u'); });
print(JSON.stringify(out));
