// Runs search_ops_cases.json (see search_ops_gen.py) with the Node driver. Usage: node search_ops_test.js [mongodb-uri]
'use strict';
const fs = require('fs');
const path = require('path');
const { MongoClient, BSON } = require('mongodb');
const { EJSON } = BSON;
const uri = process.argv[2] || 'mongodb://localhost:27018/?directConnection=true';
const data = EJSON.parse(fs.readFileSync(path.join(__dirname, '..', 'search_ops_cases.json'), 'utf8'), { relaxed: false });

const num = (v) => (v && v._bsontype ? Number(v.valueOf()) : v);
function norm(v, drop) {
  if (v === null || v === undefined) return null;
  if (typeof v === 'boolean' || typeof v === 'string') return v;
  if (typeof v === 'number') return Math.round(v * 1e9) / 1e9;
  if (v instanceof Date) return ['date', v.getTime()];
  if (v._bsontype === 'Int32' || v._bsontype === 'Long' || v._bsontype === 'Double') return Math.round(Number(v.valueOf()) * 1e9) / 1e9;
  if (Array.isArray(v)) return v.map((x) => norm(x, drop));
  if (typeof v === 'object') return Object.fromEntries(Object.keys(v).sort().filter((k) => !drop.includes(k)).map((k) => [k, norm(v[k], drop)]));
  return v;
}
const same = (a, b, drop = []) => JSON.stringify(norm(a, drop)) === JSON.stringify(norm(b, drop));

(async () => {
  const client = await MongoClient.connect(uri);
  const col = client.db('search_ops_test_node').collection('ops');
  await col.drop().catch(() => {});
  await col.insertMany(data.docs);
  const fails = [];
  for (const c of data.cases) {
    let out;
    try {
      out = await col.aggregate(c.pipeline).toArray();
    } catch (e) {
      if (c.kind !== 'error') fails.push(`${c.name}: unexpected error ${e.message}`);
      continue;
    }
    if (c.kind === 'error') { fails.push(`${c.name}: expected an error, got ${JSON.stringify(out)}`); continue; }
    let got = out, want = c.expect, drop = [];
    if (c.kind === 'ids') { got = out.map((d) => num(d._id)).sort((a, b) => a - b); want = want.map(num).sort((a, b) => a - b); }
    else if (c.kind === 'ordered') got = out.map((d) => num(d._id));
    else if (c.kind === 'scoresById') got = Object.fromEntries(out.map((d) => [String(num(d._id)), d.s]));
    else if (c.kind === 'docsById') got = Object.fromEntries(out.map((d) => { const { _id, ...rest } = d; return [String(num(_id)), rest]; }));
    else if (c.kind === 'docsNoScore') drop = ['score'];
    if (!same(got, want, drop)) fails.push(`${c.name}: got ${JSON.stringify(got)} want ${JSON.stringify(want)}`);
  }
  console.log(`search operator tests (node): ${data.cases.length - fails.length} passed, ${fails.length} failed`);
  fails.forEach((f) => console.log('  ' + f.slice(0, 600)));
  await client.close();
  process.exit(fails.length ? 1 : 0);
})();
