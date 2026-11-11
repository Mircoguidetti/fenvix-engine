// The Node driver reads every document once (the collection drivers_read.py --fill made). Usage: node drivers_read.js <uri>
const { MongoClient } = require(process.env.NODE_MONGODB || "mongodb");
(async () => {
  const cl = await MongoClient.connect(process.argv[2]);
  const c = cl.db("app_batch_drivers").collection("c");
  const expected = 60000 + 12;
  let bad = 0;
  for (const [name, cur] of [["find", c.find({})], ["find batchSize 50000", c.find({}).batchSize(50000)], ["aggregate", c.aggregate([{ $match: {} }])]]) {
    const ids = [];
    for await (const d of cur) ids.push(d._id);
    const ok = ids.length === expected && new Set(ids).size === expected;
    if (!ok) bad++;
    console.log(`${ok ? "PASS" : "FAIL"}  node ${name}: ${ids.length} read, ${new Set(ids).size} distinct, expected ${expected}`);
  }
  console.log("RESULT", bad ? "FAIL" : "PASS");
  await cl.close();
  process.exit(bad ? 1 : 0);
})();
