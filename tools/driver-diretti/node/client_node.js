// Cliente Node.js (driver ufficiale `mongodb`) per la prova M14. Stesso carico del cliente Python.
// Uso: node client_node.js URI SECONDI CONCORRENZA OUT.json
const { MongoClient, ObjectId } = require('mongodb');
const fs = require('fs');

const [uri, secs, conc, out] = process.argv.slice(2);
const T0 = Date.now();
const perServer = {}, perSec = {}, cmdFail = {}, appErrors = [];
const topo = { history: [] };
const hb = { ok: 0, fail: {} };
let ops = 0;

async function main() {
  const c = new MongoClient(uri, { monitorCommands: true });
  c.on('commandSucceeded', (e) => {
    if (e.commandName !== 'find' && e.commandName !== 'insert') return;
    const s = e.address; const sec = Math.floor((Date.now() - T0) / 1000);
    perServer[s] = (perServer[s] || 0) + 1;
    perSec[sec] = perSec[sec] || {}; perSec[sec][s] = (perSec[sec][s] || 0) + 1;
  });
  const starts = [];
  c.on('commandStarted', (e) => {
    if (e.commandName !== 'insert') return;
    const t = (Date.now() - T0) / 1000;
    if (process.env.M14_TRACE && t > 9 && t < 11) starts.push([t, e.address, e.requestId, e.command.txnNumber ? String(e.command.txnNumber) : null, e.command.lsid ? e.command.lsid.id.toString('hex').slice(0, 8) : null]);
  });
  c.on('commandFailed', (e) => { cmdFail[e.address] = (cmdFail[e.address] || 0) + 1; });
  c.on('topologyDescriptionChanged', (e) => {
    const d = e.newDescription;
    if (d.type !== 'Unknown') topo.type = d.type;
    topo.servers = [...d.servers.entries()].map(([a, s]) => `${a}=${s.type}`).sort();
    topo.history.push([(Date.now() - T0) / 1000, d.type, topo.servers]);
  });
  c.on('serverHeartbeatSucceeded', () => { hb.ok++; });
  c.on('serverHeartbeatFailed', (e) => { const k = String(e.failure && e.failure.message).slice(0, 100); hb.fail[k] = (hb.fail[k] || 0) + 1; });
  await c.connect();
  const coll = c.db('prova').collection('c');
  const stop = Date.now() + Number(secs) * 1000;
  async function worker() {
    while (Date.now() < stop) {
      const isRead = Math.random() < 0.8;
      try {
        if (isRead) await coll.findOne({ _id: Math.floor(Math.random() * 1000) });
        else await coll.insertOne({ _id: new ObjectId(), v: Math.random() });
        ops++;
      } catch (ex) {
        appErrors.push([(Date.now() - T0) / 1000, ex.name, (isRead ? 'find: ' : 'insert: ') + String(ex.message).slice(0, 200)]);
        await new Promise((r) => setTimeout(r, 50));
      }
    }
  }
  await Promise.all(Array.from({ length: Number(conc) }, worker));
  await c.close();
  const res = { driver: 'node', version: require('mongodb/package.json').version, ops, per_server: perServer, per_sec: perSec,
    cmd_fail: cmdFail, heartbeats: hb, insert_starts_9_11s: starts, app_errors: appErrors, topology: topo.type, servers: topo.servers, topo_history: topo.history.slice(-8) };
  fs.writeFileSync(out, JSON.stringify(res, null, 1));
  console.log(JSON.stringify({ ops, per_server: perServer, app_errors: appErrors.length, topology: topo.type }));
}
main().catch((e) => { console.error(e); process.exit(1); });
