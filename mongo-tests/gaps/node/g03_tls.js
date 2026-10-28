// Item 3 (Node driver): TLS and MONGODB-X509 against real mongod 7.0.45 and the engine on the same certificates (tls_env.py).
module.exports = ({ P, MongoClient }) => {
  const CA = 'ca.pem';
  const run = async (f) => { try { const r = await f(); return r === undefined ? 'ok' : r; } catch (e) { return { code: e.code ?? null, codeName: e.codeName ?? null, name: e.code ? undefined : e.constructor.name }; } };
  const ping = (c) => run(async () => { try { await c.db('admin').command({ ping: 1 }); } finally { await c.close(); } });
  const T = { tls: true, tlsCAFile: CA };

  P('A', 'g03.tls.crud_over_tls', async (c, db) => {
    const x = c.mk(T); const t = x.db(db.databaseName).collection('t');
    await t.insertMany(Array.from({ length: 50 }, (_, i) => ({ _id: i, v: 'x'.repeat(1000) }))); await t.insertOne({ _id: 'big', blob: 'y'.repeat(9 * 1024 * 1024) });
    await t.updateOne({ _id: 3 }, { $set: { z: 1 } });
    const r = { n: await t.countDocuments({}), z: (await t.findOne({ _id: 3 })).z, big: (await t.findOne({ _id: 'big' })).blob.length, agg: await t.aggregate([{ $group: { _id: null, n: { $sum: 1 } } }]).toArray() };
    await x.close(); return r;
  });
  P('A', 'g03.tls.uri_options', async (c, db) => {
    const [h, p] = c.gaps_addr; const ca = c.mk && require('path').join(__dirname, '..', '..', '..', '..', '.tools', 'certs', CA); const r = {};
    for (const [k, q] of Object.entries({ tls: `tls=true&tlsCAFile=${ca}`, insecure: 'tls=true&tlsInsecure=true', allowInvalid: 'tls=true&tlsAllowInvalidCertificates=true', no_ca: 'tls=true', plain: 'tls=false' })) r[k] = await ping(new MongoClient(`mongodb://${h}:${p}/?${q}`, { serverSelectionTimeoutMS: 3000 }));
    return r;
  });
  P('A', 'g03.tls.wrong_ca_and_plain_client', async (c) => ({ other_ca: await ping(c.mk({ tls: true, tlsCAFile: 'other-ca.pem' })), plain: await ping(c.mk()), allow_invalid: await ping(c.mk({ tls: true, tlsAllowInvalidCertificates: true })), insecure: await ping(c.mk({ tls: true, tlsInsecure: true })) }));
  P('A', 'g03.tls.hostname_verification', async (c) => ({ ip_in_san: await ping(c.mk(T, '127.0.0.1')), san_name: await ping(c.mk(T, 'node1.example.test')), not_in_san: await ping(c.mk(T, 'node9.example.test')), allowed: await ping(c.mk({ ...T, tlsAllowInvalidHostnames: true }, 'node9.example.test')) }));
  P('A', 'g03.tls.optional_client_certificate', async (c) => ({ no_cert: await ping(c.mk(T)), with_cert: await ping(c.mk({ ...T, tlsCertificateKeyFile: 'client.pem' })), untrusted: await ping(c.mk({ ...T, tlsCertificateKeyFile: 'untrusted-client.pem' })) }));
  P('B', 'g03.tls.self_signed_server_certificate', async (c, db) => {
    const x = c.mk({ tls: true, tlsCAFile: 'selfsigned.pem' }); await x.db(db.databaseName).collection('t').insertOne({ _id: 1 }); const f = await x.db(db.databaseName).collection('t').find().toArray(); await x.close();
    return { f, trusted: await ping(c.mk({ tls: true, tlsCAFile: 'selfsigned.pem' })), wrong_ca: await ping(c.mk(T)), plain: await ping(c.mk()), insecure: await ping(c.mk({ tls: true, tlsInsecure: true })) };
  });
  P('C', 'g03.tls.client_certificate_required', async (c) => ({ no_cert: await ping(c.mk(T)), client: await ping(c.mk({ ...T, tlsCertificateKeyFile: 'client.pem' })), client2: await ping(c.mk({ ...T, tlsCertificateKeyFile: 'client2.pem' })), untrusted: await ping(c.mk({ ...T, tlsCertificateKeyFile: 'untrusted-client.pem' })), plain: await ping(c.mk()) }));
  P('D', 'g03.tls.preferTLS_accepts_both', async (c, db) => {
    const p = c.mk(); const t = c.mk(T);
    await p.db(db.databaseName).collection('t').insertOne({ _id: 1, via: 'plain' }); await t.db(db.databaseName).collection('t').insertOne({ _id: 2, via: 'tls' });
    const both = [(await p.db(db.databaseName).collection('t').find().toArray()).map((d) => d._id), (await t.db(db.databaseName).collection('t').find().toArray()).map((d) => d._id)];
    await p.close(); await t.close(); return { both, plain: await ping(c.mk()), tls: await ping(c.mk(T)) };
  });
  const SUBJECT = 'CN=client1,OU=Dev,O=Clients,C=US';
  P('E', 'g03.x509.authenticate_with_client_certificate', async (c, db) => {
    const root = c.mk({ ...T, tlsCertificateKeyFile: 'client.pem', auth: { username: 'root', password: 'rootpw' }, authSource: 'admin' });
    const out = {}; const X = (extra = {}) => c.mk({ ...T, tlsCertificateKeyFile: 'client.pem', authMechanism: 'MONGODB-X509', authSource: '$external', ...extra });
    await root.db('$external').command({ createUser: SUBJECT, roles: [{ role: 'readWrite', db: db.databaseName }] });
    const x = X(); out.status = (await x.db('admin').command({ connectionStatus: 1 })).authInfo.authenticatedUsers;
    out.own = await run(() => x.db(db.databaseName).collection('t').insertOne({ _id: 1 })); out.other = await run(() => x.db(db.databaseName + '_o').collection('t').insertOne({ _id: 1 })); await x.close();
    out.no_user_cert = await ping(c.mk({ ...T, tlsCertificateKeyFile: 'client2.pem', authMechanism: 'MONGODB-X509', authSource: '$external' }));
    out.explicit_user = await ping(X({ auth: { username: SUBJECT } })); out.wrong_user = await ping(X({ auth: { username: 'CN=someone' } }));
    const nc = c.mk({ ...T, tlsCertificateKeyFile: 'client.pem' }); out.cert_only = await run(() => nc.db(db.databaseName).collection('t').findOne()); await nc.close();
    await root.db('$external').command({ dropUser: SUBJECT }); out.after_drop = await ping(X()); await root.close();
    return out;
  });
  P('E', 'g03.tls.scram_over_tls_with_auth', async (c, db) => {
    const root = c.mk({ ...T, tlsCertificateKeyFile: 'client.pem', auth: { username: 'root', password: 'rootpw' }, authSource: 'admin' });
    await root.db(db.databaseName).command({ createUser: 'tu', pwd: 'pw', roles: [{ role: 'read', db: db.databaseName }] });
    const u = c.mk({ ...T, tlsCertificateKeyFile: 'client.pem', auth: { username: 'tu', password: 'pw' }, authSource: db.databaseName });
    const out = { find: await run(() => u.db(db.databaseName).collection('t').find().toArray().then(() => undefined)), insert: await run(() => u.db(db.databaseName).collection('t').insertOne({ a: 1 })) };
    await u.close(); await root.db(db.databaseName).command({ dropAllUsersFromDatabase: 1 }); await root.close(); return out;
  });

  // ---- mongodb+srv://  (Node resolves SRV/TXT with c-ares: point it at the local DNS server)
  require('dns').setServers(['127.0.0.1:5353']);
  const NAMES = { 27040: 'areal', 27048: 'aours', 27041: 'breal', 27049: 'bours', 27042: 'creal', 27050: 'cours', 27043: 'dreal', 27051: 'dours', 27044: 'ereal', 27052: 'eours' };
  const srv = (c, q = '', name, creds = '') => new MongoClient(`mongodb+srv://${creds}${name || NAMES[c.gaps_addr[1]]}.example.test/?${q}`, { serverSelectionTimeoutMS: 4000 });
  const caq = `tlsCAFile=${require('path').join(__dirname, '..', '..', '..', '..', '.tools', 'certs', CA)}`;
  const certq = (n) => `tlsCertificateKeyFile=${require('path').join(__dirname, '..', '..', '..', '..', '.tools', 'certs', n)}`;
  P('A', 'g03.srv.basic_and_defaults', async (c, db) => {
    const x = srv(c, caq); await x.db(db.databaseName).collection('t').insertOne({ _id: 1 });
    const r = { find: await x.db(db.databaseName).collection('t').find().toArray(), tls_default_on: await ping(srv(c, '')), tls_off: await ping(srv(c, 'tls=false')), ssl_off: await ping(srv(c, 'ssl=false')) };
    await x.close(); return r;
  });
  P('D', 'g03.srv.preferTLS_server_with_tls_false', async (c) => ({ tls: await ping(srv(c, caq)), plain: await ping(srv(c, 'tls=false')) }));
  P('E', 'g03.srv.txt_record_supplies_authSource', async (c, db) => {
    const root = c.mk({ ...T, tlsCertificateKeyFile: 'client.pem', auth: { username: 'root', password: 'rootpw' }, authSource: 'admin' });
    await root.db(db.databaseName).command({ createUser: 'su', pwd: 'pw', roles: [{ role: 'read', db: db.databaseName }] });
    const q = `${caq}&${certq('client.pem')}`;
    const out = { root: await ping(srv(c, q, undefined, 'root:rootpw@')), other_db: await ping(srv(c, `${q}&authSource=${db.databaseName}`, undefined, 'su:pw@')), default_source: await ping(srv(c, q, undefined, 'su:pw@')) };
    await root.db(db.databaseName).command({ dropAllUsersFromDatabase: 1 }); await root.close(); return out;
  });
  P('A', 'g03.srv.errors', async (c) => {
    const real = c.gaps_addr[1] === 27040; const w = real ? 'real' : 'ours';
    return { no_such_name: await ping(new MongoClient(`mongodb+srv://nosuch.example.test/?${caq}`, { serverSelectionTimeoutMS: 3000 })), outside: await ping(new MongoClient(`mongodb+srv://outside.example.test/?${caq}`, { serverSelectionTimeoutMS: 3000 })),
      badtxt: await ping(new MongoClient(`mongodb+srv://badtxt${w}.example.test/?${caq}`, { serverSelectionTimeoutMS: 3000 })), service: await ping(new MongoClient(`mongodb+srv://custom${w}.example.test/?${caq}&srvServiceName=custom`, { serverSelectionTimeoutMS: 3000 })) };
  });
};
