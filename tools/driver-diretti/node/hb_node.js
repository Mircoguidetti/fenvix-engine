// Battiti del driver Node verso i camerieri: riusciti, falliti (con l'errore) e se in modalità streaming.
const { MongoClient } = require('mongodb');
(async () => {
  const hb = { ok: 0, fail: {}, awaited: 0 };
  const c = new MongoClient(process.argv[2], { heartbeatFrequencyMS: Number(process.argv[3] || 10000), serverMonitoringMode: process.argv[4] || 'auto' });
  c.on('serverHeartbeatSucceeded', (e) => { hb.ok++; if (e.awaited) hb.awaited++; });
  c.on('serverHeartbeatFailed', (e) => { const k = String(e.failure && e.failure.message).slice(0, 120); hb.fail[k] = (hb.fail[k] || 0) + 1; });
  await c.connect();
  await new Promise((r) => setTimeout(r, Number(process.argv[5] || 8000)));
  await c.close();
  console.log(JSON.stringify(hb));
})();
