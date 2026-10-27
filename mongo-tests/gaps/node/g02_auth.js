// Item 2 (Node driver): authentication, user/role management and role enforcement against servers started with --auth.
module.exports = ({ A, MongoClient }) => {
  const uniq = () => Math.random().toString(16).slice(2, 8);
  const out = async (f) => { try { const r = await f(); return r === undefined ? 'ok' : r; } catch (e) { return { code: e.code ?? null, codeName: e.codeName ?? null, name: e.code ? undefined : e.name }; } };
  const hostport = (c) => { const h = c.options.hosts[0]; return `${h.host}:${h.port}`; };
  const login = async (c, user, pw, source = 'admin', extra = '') => { const m = new MongoClient(`mongodb://${encodeURIComponent(user)}:${encodeURIComponent(pw)}@${hostport(c)}/?authSource=${source}${extra}`, { serverSelectionTimeoutMS: 8000 }); return m; };
  const mkUser = async (c, db, { name = 'u' + uniq(), pw = 'pw1', roles = [], extra = {} } = {}) => { await c.db(db).command({ createUser: name, pwd: pw, roles, ...extra }); return name; };
  const canFind = (c, db, coll = 't') => out(async () => { await c.db(db).collection(coll).find().limit(1).toArray(); });

  A('g02.login.scram_default_and_explicit', async (c, db) => {
    const u = await mkUser(c, 'admin', { roles: [{ role: 'root', db: 'admin' }] });
    const r = {};
    for (const m of ['', '&authMechanism=SCRAM-SHA-1', '&authMechanism=SCRAM-SHA-256']) { const x = await login(c, u, 'pw1', 'admin', m); r[m || 'default'] = await out(async () => { await x.db('admin').command({ ping: 1 }); }); await x.close(); }
    return r;
  });
  A('g02.login.wrong_password_unknown_user', async (c, db) => {
    const u = await mkUser(c, 'admin', { roles: [{ role: 'root', db: 'admin' }] });
    const r = {};
    for (const m of ['', '&authMechanism=SCRAM-SHA-1', '&authMechanism=SCRAM-SHA-256']) {
      const a = await login(c, u, 'bad', 'admin', m); r['bad' + m] = await out(async () => { await a.db('admin').command({ ping: 1 }); }); await a.close();
      const b = await login(c, 'nouser' + uniq(), 'x', 'admin', m); r['unk' + m] = await out(async () => { await b.db('admin').command({ ping: 1 }); }); await b.close();
    }
    return r;
  });
  A('g02.login.authSource_per_database_users', async (c, db) => {
    const n = 'dup' + uniq();
    await mkUser(c, db.databaseName, { name: n, pw: 'one', roles: [{ role: 'read', db: db.databaseName }] });
    await mkUser(c, 'admin', { name: n, pw: 'two', roles: [{ role: 'root', db: 'admin' }] });
    const r = {};
    for (const [pw, src] of [['one', db.databaseName], ['two', db.databaseName], ['two', 'admin'], ['one', 'admin'], ['one', 'zzz']]) { const x = await login(c, n, pw, src); r[`${pw}@${src === db.databaseName ? 'db' : src}`] = await out(async () => { await x.db('admin').command({ ping: 1 }); }); await x.close(); }
    return r;
  });
  A('g02.login.special_password', async (c, db) => {
    const u = await mkUser(c, 'admin', { pw: 'p@ss:w/ord%?é', roles: [{ role: 'root', db: 'admin' }] });
    const r = {};
    for (const m of ['', '&authMechanism=SCRAM-SHA-1']) { const x = await login(c, u, 'p@ss:w/ord%?é', 'admin', m); r[m || 'default'] = await out(async () => { await x.db('admin').command({ ping: 1 }); }); await x.close(); }
    return r;
  });
  A('g02.login.mechanism_restricted_user', async (c, db) => {
    const u = await mkUser(c, 'admin', { roles: [{ role: 'root', db: 'admin' }], extra: { mechanisms: ['SCRAM-SHA-1'] } });
    const r = {};
    for (const m of ['', '&authMechanism=SCRAM-SHA-1', '&authMechanism=SCRAM-SHA-256']) { const x = await login(c, u, 'pw1', 'admin', m); r[m || 'default'] = await out(async () => { await x.db('admin').command({ ping: 1 }); }); await x.close(); }
    return r;
  });
  A('g02.unauth.commands', async (c, db) => {
    const x = new MongoClient(`mongodb://${hostport(c)}`, { serverSelectionTimeoutMS: 8000 }); await x.connect();
    const d = x.db(db.databaseName); const r = {};
    for (const [k, f] of Object.entries({ ping: () => x.db('admin').command({ ping: 1 }), find: () => d.collection('t').find().toArray(), insert: () => d.collection('t').insertOne({ a: 1 }), listDatabases: () => x.db('admin').command({ listDatabases: 1 }), createUser: () => d.command({ createUser: 'x', pwd: 'y', roles: [] }), usersInfo: () => x.db('admin').command({ usersInfo: 1 }), connectionStatus: async () => (await x.db('admin').command({ connectionStatus: 1 })).authInfo.authenticatedUsers.length })) r[k] = await out(async () => { const v = await f(); return typeof v === 'number' ? v : undefined; });
    await x.close(); return r;
  });
  A('g02.users.lifecycle', async (c, db) => {
    const d = c.db(db.databaseName); const roles = async () => (await d.command({ usersInfo: 'carol' })).users[0].roles.map((r) => `${r.role}@${r.db}`).sort();
    const r = {};
    await d.command({ createUser: 'carol', pwd: 'p', roles: [], customData: { a: 1 } }); r.start = await roles();
    await d.command({ grantRolesToUser: 'carol', roles: ['read', { role: 'readWrite', db: db.databaseName }] }); r.granted = await roles();
    await d.command({ revokeRolesFromUser: 'carol', roles: ['read'] }); r.revoked = await roles();
    await d.command({ updateUser: 'carol', roles: [{ role: 'dbAdmin', db: db.databaseName }] }); r.updated = await roles();
    r.dup = await out(() => d.command({ createUser: 'carol', pwd: 'p', roles: [] }));
    r.badRole = await out(() => d.command({ grantRolesToUser: 'carol', roles: ['nosuch'] }));
    r.drop = await out(() => d.command({ dropUser: 'carol' })); r.dropAgain = await out(() => d.command({ dropUser: 'carol' }));
    r.info = (await d.command({ usersInfo: 1 })).users.length;
    return r;
  });
  A('g02.roles.custom_role_enforced', async (c, db) => {
    const d = c.db(db.databaseName); await d.collection('t').insertMany([{ _id: 1 }, { _id: 2 }]);
    await d.command({ createRole: 'reader_t', privileges: [{ resource: { db: db.databaseName, collection: 't' }, actions: ['find'] }], roles: [] });
    const u = await mkUser(c, db.databaseName, { roles: ['reader_t'] }); const x = await login(c, u, 'pw1', db.databaseName); await x.connect();
    const r = { find_t: await canFind(x, db.databaseName, 't'), find_other: await canFind(x, db.databaseName, 'other'), insert: await out(async () => { await x.db(db.databaseName).collection('t').insertOne({ _id: 9 }); }) };
    await d.command({ grantRolesToRole: 'reader_t', roles: ['readWrite'] }); r.insert_after_grant = await out(async () => { await x.db(db.databaseName).collection('t').insertOne({ _id: 10 }); });
    await x.close(); return r;
  });
  for (const [role, where] of [['read', 'db'], ['readWrite', 'db'], ['dbAdmin', 'db'], ['userAdmin', 'db'], ['dbOwner', 'db'], ['readAnyDatabase', 'admin'], ['readWriteAnyDatabase', 'admin'], ['clusterAdmin', 'admin'], ['root', 'admin']]) {
    A(`g02.matrix.${role}`, async (c, db) => {
      const dbn = db.databaseName; const other = dbn + '_o';
      await c.db(dbn).collection('t').insertOne({ _id: 1, x: 1 }); await c.db(dbn).collection('t2').insertOne({ _id: 1, x: 1 }); await c.db(other).collection('t').insertOne({ _id: 1 });
      const rdb = where === 'db' ? dbn : 'admin'; const u = await mkUser(c, rdb, { roles: [{ role, db: rdb }] });
      const x = await login(c, u, 'pw1', rdb); await x.connect(); const d = x.db(dbn); const o = x.db(other);
      const ops = {
        find: async () => { await d.collection('t').find().limit(1).toArray(); }, find_other_db: async () => { await o.collection('t').find().limit(1).toArray(); },
        insert: async () => { await d.collection('t').insertOne({ x: 1 }); }, insert_other_db: async () => { await o.collection('t').insertOne({ x: 1 }); },
        update: async () => { await d.collection('t').updateOne({}, { $set: { y: 1 } }); }, delete: async () => { await d.collection('t').deleteOne({ zz: 1 }); },
        count: async () => { await d.collection('t').countDocuments({}); }, aggregate_out: async () => { await d.collection('t').aggregate([{ $match: {} }, { $out: 'outc' }]).toArray(); },
        createCollection: async () => { await d.createCollection('newc'); }, createIndex: async () => { await d.collection('t').createIndex({ x: 1 }); },
        listIndexes: async () => { await d.collection('t').indexes(); }, listCollections: async () => { await d.listCollections().toArray(); },
        collStats: async () => { await d.command({ collStats: 't' }); }, dbStats: async () => { await d.command({ dbStats: 1 }); },
        listDatabases: async () => { await x.db('admin').command({ listDatabases: 1 }); },
        createUser: async () => { await d.command({ createUser: 'nu' + uniq(), pwd: 'p', roles: [] }); }, usersInfo: async () => { await d.command({ usersInfo: 1 }); },
        dropCollection: async () => { await d.collection('t2').drop(); }, dropDatabase: async () => { await o.dropDatabase(); },
      };
      const res = {}; for (const [k, f] of Object.entries(ops)) res[k] = await out(f);
      await x.close(); await c.db(other).dropDatabase(); return res;
    });
  }
  A('g02.login.authenticationRestrictions', async (c, db) => {
    const d = c.db(db.databaseName); const r = {};
    r.bad = await out(() => d.command({ createUser: 'r0', pwd: 'p', roles: [], authenticationRestrictions: [{ clientSource: ['not-an-ip'] }] }));
    const specs = { r1: [{ clientSource: ['10.1.2.3'] }], r2: [{ clientSource: ['127.0.0.0/8'], serverAddress: ['127.0.0.1'] }], r3: [{ clientSource: ['10.1.2.3'] }, { serverAddress: ['127.0.0.1/32'] }], r4: [{ clientSource: ['127.0.0.1'], serverAddress: ['10.0.0.1'] }] };
    for (const [u, rs] of Object.entries(specs)) { await d.command({ createUser: u, pwd: 'p', roles: [], authenticationRestrictions: rs }); const x = await login(c, u, 'p', db.databaseName); r[u] = await out(async () => { await x.db('admin').command({ ping: 1 }); }); await x.close(); }
    return r;
  });
};
