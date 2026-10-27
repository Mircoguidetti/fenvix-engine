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
const C = (name, fn, o = {}) => CASES.push({ name, fn, rs: !!o.rs, auth: !!o.auth, pair: o.pair || null });
const sortKeys = (x) => Array.isArray(x) ? x.map(sortKeys) : (x && typeof x === 'object' && !(x instanceof Date) && !x._bsontype && !Buffer.isBuffer(x)) ? Object.fromEntries(Object.keys(x).sort().map((k) => [k, sortKeys(x[k])])) : x;
const canon = (x, sk) => EJSON.stringify(sk ? sortKeys(x === undefined ? null : x) : (x === undefined ? null : x), { relaxed: false });
const errOf = (e) => ({ __error__: { code: e.code ?? null, codeName: e.codeName ?? null, name: e.code ? undefined : e.name } });
const H = { C, canon, errOf, MongoClient, ObjectId, Long, EJSON, args };
H.A = (name, fn, o = {}) => C(name, fn, { ...o, auth: true });
H.P = (pair, name, fn) => C(name, fn, { pair });
const PAIRS = { A: [27040, 27048], B: [27041, 27049], C: [27042, 27050], D: [27043, 27051], E: [27044, 27052] };
const CERTS = path.join(__dirname, '..', '..', '..', '.tools', 'certs');
H.cert = (n) => path.join(CERTS, n);
// a client factory for one server of a TLS pair: mk({tls:true, tlsCAFile:'ca.pem'}, host) -> MongoClient (file names are resolved in CERTS)
const pairClient = (port) => ({ gaps_addr: ['localhost', port], mk: (opts = {}, host) => { const o = Object.fromEntries(Object.entries(opts).map(([k, v]) => [k, typeof v === 'string' && v.endsWith('.pem') ? path.join(CERTS, v) : v])); return new MongoClient(`mongodb://${host || 'localhost'}:${port}/`, { serverSelectionTimeoutMS: 3000, ...o }); } });

const deepDiff = (a, b, p = '') => { const out = []; if (a && b && typeof a === 'object' && typeof b === 'object') { for (const k of new Set([...Object.keys(a), ...Object.keys(b)])) { if (!(k in a)) out.push(`${p}/${k}: only ours = ${JSON.stringify(b[k]).slice(0, 150)}`); else if (!(k in b)) out.push(`${p}/${k}: only real = ${JSON.stringify(a[k]).slice(0, 150)}`); else out.push(...deepDiff(a[k], b[k], `${p}/${k}`)); } } else if (JSON.stringify(a) !== JSON.stringify(b)) out.push(`${p}: real=${JSON.stringify(a)?.slice(0, 150)} ours=${JSON.stringify(b)?.slice(0, 150)}`); return out; };

async function runOn(client, c) {
  const dbname = 'gn_' + Math.random().toString(16).slice(2, 12);
  const db = c.pair ? { databaseName: dbname, dropDatabase: async () => {} } : client.db(dbname);
  try {
    return canon(await Promise.race([c.fn(client, db), new Promise((_, rej) => setTimeout(() => rej(Object.assign(new Error('timeout 40s'), { code: 'TIMEOUT' })), 40000))])).split(dbname).join('$DB');
  } catch (e) { return canon(errOf(e)); } finally { try { await db.dropDatabase(); } catch (e) { /* ignore */ } }
}

(async () => {
  for (const f of fs.readdirSync(__dirname).filter((f) => /^g\d.*\.js$/.test(f)).sort()) {
    if (args.item && !f.startsWith(args.item)) continue;
    require(path.join(__dirname, f))(H);
  }
  const mk = async (uri) => { const c = await new MongoClient(uri, { serverSelectionTimeoutMS: 4000 }).connect(); c.__uri = uri; return c; };
  const only = (pred) => CASES.filter((c) => !args.only || c.name.includes(args.only)).some(pred);
  const need = only((c) => !c.pair && !c.auth);
  const real = need ? await mk(args.real || 'mongodb://localhost:27017') : null;
  const realRs = need ? await mk(args['real-rs'] || 'mongodb://localhost:27020/?replicaSet=rs0') : null;
  const ours = need ? await mk(args.ours || 'mongodb://localhost:27018') : null;
  const oursRs = args['ours-rs'] ? await mk(args['ours-rs']) : ours;
  const realA = args['real-auth'] === 'skip' ? null : await mk(args['real-auth'] || 'mongodb://root:rootpw@localhost:27030/?authSource=admin').catch(() => null);
  const oursA = args['real-auth'] === 'skip' ? null : await mk(args['ours-auth'] || 'mongodb://root:rootpw@localhost:27028/?authSource=admin').catch(() => null);
  let n = 0, bad = 0;
  for (const c of CASES) {
    if (args.only && !c.name.includes(args.only)) continue;
    if (c.auth && !(realA && oursA)) { console.log('S', c.name, '(auth servers not running)'); continue; }
    const r = c.pair ? await runOn(pairClient(PAIRS[c.pair][0]), c) : await runOn(c.auth ? realA : c.rs ? realRs : real, c);
    const o = c.pair ? await runOn(pairClient(PAIRS[c.pair][1]), c) : await runOn(c.auth ? oursA : c.rs ? oursRs : ours, c);
    n++; if (r !== o) bad++;
    console.log(r === o ? '.' : 'F', c.name);
    if (r !== o) { try { for (const d of deepDiff(JSON.parse(r), JSON.parse(o)).slice(0, 8)) console.log('    diff', d); } catch (e) { console.log('    real:', r.slice(0, 400), '\n    ours:', o.slice(0, 400)); } }
  }
  console.log(`gaps_node: ${n - bad}/${n} identical to mongod 7.0.45`);
  await Promise.all([real, realRs, ours, realA, oursA].filter(Boolean).map((c) => c.close()).concat(oursRs && oursRs !== ours ? [oursRs.close()] : []));
  process.exit(bad ? 1 : 0);
})().catch((e) => { console.error(e); process.exit(1); });
