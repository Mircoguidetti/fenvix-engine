#!/usr/bin/env node
// Differential tests for the compat-gaps work (Node.js driver). A case is `C(name, async (client, db) => result, {rs, uri})`; it runs on
// real mongod 7.0 and on the engine (fresh database each) and the results/errors (EJSON canonical) must be identical.
// usage: node gaps_node.js [--only=substr] [--item=g01] [--real=uri] [--real-rs=uri] [--ours=uri] [--ours-rs=uri]
'use strict';
const path = require('path');
const fs = require('fs');
const { createRequire } = require('module');
const req = createRequire(path.join(__dirname, '../../node/package.json'));
const { MongoClient, ObjectId, Long } = req('mongodb');
const { EJSON } = req('bson');

const args = Object.fromEntries(process.argv.slice(2).map((a) => a.replace(/^--/, '')).map((a) => { const i = a.indexOf('='); return i < 0 ? [a, true] : [a.slice(0, i), a.slice(i + 1)]; }));
const CASES = [];
const C = (name, fn, o = {}) => CASES.push({ name, fn, rs: !!o.rs });
const sortKeys = (x) => Array.isArray(x) ? x.map(sortKeys) : (x && typeof x === 'object' && !(x instanceof Date) && !x._bsontype && !Buffer.isBuffer(x)) ? Object.fromEntries(Object.keys(x).sort().map((k) => [k, sortKeys(x[k])])) : x;
const canon = (x, sk) => EJSON.stringify(sk ? sortKeys(x === undefined ? null : x) : (x === undefined ? null : x), { relaxed: false });
const errOf = (e) => ({ __error__: { code: e.code ?? null, codeName: e.codeName ?? null, name: e.code ? undefined : e.name } });
const H = { C, canon, errOf, MongoClient, ObjectId, Long, EJSON, args };

async function runOn(client, c) {
  const dbname = 'gn_' + Math.random().toString(16).slice(2, 12);
  const db = client.db(dbname);
  try {
    return canon(await Promise.race([c.fn(client, db), new Promise((_, rej) => setTimeout(() => rej(Object.assign(new Error('timeout 40s'), { code: 'TIMEOUT' })), 40000))])).split(dbname).join('$DB');
  } catch (e) { return canon(errOf(e)); } finally { try { await db.dropDatabase(); } catch (e) { /* ignore */ } }
}

(async () => {
  for (const f of fs.readdirSync(__dirname).filter((f) => /^g\d.*\.js$/.test(f)).sort()) {
    if (args.item && !f.startsWith(args.item)) continue;
    require(path.join(__dirname, f))(H);
  }
  const mk = async (uri) => { const c = await new MongoClient(uri).connect(); c.__uri = uri; return c; };
  const real = await mk(args.real || 'mongodb://localhost:27017');
  const realRs = await mk(args['real-rs'] || 'mongodb://localhost:27020/?replicaSet=rs0');
  const ours = await mk(args.ours || 'mongodb://localhost:27018');
  const oursRs = args['ours-rs'] ? await mk(args['ours-rs']) : ours;
  let n = 0, bad = 0;
  for (const c of CASES) {
    if (args.only && !c.name.includes(args.only)) continue;
    const r = await runOn(c.rs ? realRs : real, c); const o = await runOn(c.rs ? oursRs : ours, c);
    n++; if (r !== o) bad++;
    console.log(r === o ? '.' : 'F', c.name);
    if (r !== o) console.log('    real:', r.slice(0, 400), '\n    ours:', o.slice(0, 400));
  }
  console.log(`gaps_node: ${n - bad}/${n} identical to mongod 7.0.45`);
  await Promise.all([real.close(), realRs.close(), ours.close()].concat(oursRs !== ours ? [oursRs.close()] : []));
  process.exit(bad ? 1 : 0);
})().catch((e) => { console.error(e); process.exit(1); });
