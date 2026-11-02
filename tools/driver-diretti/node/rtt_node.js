// Quanto vede il driver Node il giro di ogni cameriere (roundTripTime della descrizione del server), ogni secondo.
const { MongoClient } = require('mongodb');
(async () => {
  const c = new MongoClient(process.argv[2], { heartbeatFrequencyMS: 500 });
  await c.connect();
  for (let i = 0; i < 8; i++) {
    await new Promise((r) => setTimeout(r, 1000));
    const d = c.topology.description;
    console.log(JSON.stringify([...d.servers.entries()].map(([a, s]) => [a, Math.round(s.roundTripTime * 100) / 100, s.type])));
  }
  await c.close();
})();
