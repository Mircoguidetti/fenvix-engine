// Item 10 (Node driver): command-level differential scripts, the same scripts/*.json as g10_scripts.py (see there for the format).
const fs = require('fs');
const path = require('path');
module.exports = ({ C, EJSON }) => {
  const PLAN = 'PlanExecutor error during aggregation :: caused by :: ';
  const UUID_RE = /[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}/g;
  const VOLATILE = ['$clusterTime', 'operationTime', 'electionId', 'opTime'];
  const subst = (x, db, other) => {
    if (typeof x === 'string') return x.split('$OTHER').join(other).split('$DB').join(db);
    if (Array.isArray(x)) return x.map((v) => subst(v, db, other));
    if (x && typeof x === 'object') return Object.fromEntries(Object.entries(x).map(([k, v]) => [subst(k, db, other), subst(v, db, other)]));
    return x;
  };
  const strip = (x, names) => {
    if (Array.isArray(x)) return x.map((v) => strip(v, names));
    if (x && typeof x === 'object' && !x._bsontype && !(x instanceof Date) && !Buffer.isBuffer(x)) return Object.fromEntries(Object.entries(x).filter(([k]) => !names.includes(k)).map(([k, v]) => [k, strip(v, names)]));
    return x;
  };
  const unify = (x) => {
    if (Array.isArray(x)) return x.map(unify);
    if (typeof x === 'string') return x.replace(UUID_RE, '<uuid>').replace(PLAN, '').replace(/(transaction number |txnNumber: |active transaction number is )-?\d+/g, '$1<n>'); // (the driver's own session numbers its transactions: client state, not server behaviour)
    if (x && typeof x === 'object' && !x._bsontype && !(x instanceof Date) && !Buffer.isBuffer(x)) return Object.fromEntries(Object.entries(x).map(([k, v]) => [k, (k === 'uuid' || k === 'collectionUUID') && v && v._bsontype === 'Binary' && v.sub_type === 4 ? '<uuid>' : unify(v)]));
    return x;
  };
  const norm = (r, names) => { r = strip(r, [...VOLATILE, ...names]); if (r.cursor && 'id' in r.cursor) r.cursor.id = r.cursor.id.toString() !== '0' ? 'open' : 'closed'; return unify(r); };
  const sortarr = (rec, path) => { const ks = path.split('.'); let n = rec; for (const k of ks.slice(0, -1)) n = n[k]; const key = (x) => (x && typeof x === 'object' && 'name' in x) ? String(x.name) : JSON.stringify(x); n[ks[ks.length - 1]].sort((a, b) => { const ka = key(a), kb = key(b); return ka < kb ? -1 : ka > kb ? 1 : 0; }); return rec; };
  const shape = (x) => { if (Array.isArray(x)) return x.length ? [shape(x[0])] : []; if (x && x._bsontype) return 'num'; if (x instanceof Date) return 'Date'; if (x && typeof x === 'object') return Object.fromEntries(Object.entries(x).map(([k, v]) => [k, shape(v)])); return typeof x === 'number' ? 'num' : typeof x; };
  const sleep = (ms) => new Promise((r) => setTimeout(r, ms));
  for (const f of fs.readdirSync(path.join(__dirname, '..', 'scripts')).filter((f) => f.endsWith('.json')).sort()) {
    for (const c of JSON.parse(fs.readFileSync(path.join(__dirname, '..', 'scripts', f), 'utf8'))) {
      const mode = c.mode || 'std';
      C(c.name, async (client, db) => {
        const other = client.db(db.databaseName + '_x'); const out = {}; const lsid = { id: { $binary: { base64: require('crypto').randomBytes(16).toString('base64'), subType: '04' } } };
        try {
          for (let i = 0; i < c.steps.length; i++) {
            const st = { ...c.steps[i] }; const opts = {};
            for (const k of Object.keys(st)) if (k.startsWith('__')) { opts[k] = st[k]; delete st[k]; }
            if (opts.__pause) await sleep(opts.__pause);
            const target = opts.__db === 'admin' ? client.db('admin') : opts.__db === '$OTHER' ? other : db;
            const sub = subst(st, db.databaseName, other.databaseName); if (sub.lsid === '$LSID') sub.lsid = lsid;
            const cmd = EJSON.parse(JSON.stringify(sub), { relaxed: false });
            let rec;
            try { rec = norm(await target.command(cmd), opts.__strip || []); if (opts.__sortarr) rec = sortarr(rec, opts.__sortarr); if (opts.__shape) rec = shape(rec); } catch (e) { rec = (opts.__expect_close && !e.codeName && !e.code) ? { connection: e.constructor.name === 'MongoServerSelectionError' || /ECONNREFUSED|closed|ENOTFOUND/.test(e.message) ? 'closed' : e.constructor.name } : { err: [e.code, e.codeName, unify(e.errmsg || e.message || '')] }; }
            if (process.env.GAPS_TRACE && c.name.includes(process.env.GAPS_TRACE)) console.log('TRACE', c.name, i, client.s.url.slice(-30), JSON.stringify(cmd).slice(0, 200), JSON.stringify(rec).slice(0, 300));
            if (!opts.__skip) out[opts.__as || `s${i}`] = rec;
          }
          return out;
        } finally { await other.dropDatabase().catch(() => {}); }
      }, { rs: mode === 'rs', sa: mode === 'sa', fresh: mode.startsWith('fresh:') ? mode.split(':')[1] : null });
    }
  }
};
