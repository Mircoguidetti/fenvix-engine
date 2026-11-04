// Review C12: does the Node driver retry a 134 (RetryableWriteError) by itself? Prints attempts and the final error.
const { MongoClient } = require("mongodb");
(async () => {
  const c = new MongoClient(process.argv[2], { retryWrites: true, monitorCommands: true, serverSelectionTimeoutMS: 30000, directConnection: true });
  const seen = [];
  c.on("commandStarted", (e) => { if (e.commandName === "insert") seen.push(String(e.command.txnNumber)); });
  let err = null;
  try { await c.db("ct").collection("c").insertOne({ x: 1 }); } catch (e) { err = { code: e.code, labels: e.errorLabels || (e.errorLabelSet ? [...e.errorLabelSet] : null) }; }
  console.log(JSON.stringify({ attempts: seen.length, txnNumbers: seen, err }));
  await c.close();
})();
