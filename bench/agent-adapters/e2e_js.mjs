// End-to-end check of one JS adapter against the target on localhost:27017 (same loop as e2e_py.py):
//   node e2e_js.mjs langgraph-js|mastra OUT.json
// run_e2e.sh copies this file into the adapter's install directory so the packages resolve. Fake model, no paid model.
import { createHash } from 'node:crypto';
import { writeFileSync } from 'node:fs';

const URI = process.env.E2E_URI || 'mongodb://localhost:27017/?directConnection=true';
const USERS = Number(process.env.E2E_USERS || 50);
const TURNS = Number(process.env.E2E_TURNS || 200);
const WORKERS = Number(process.env.E2E_WORKERS || 10);

const userText = (u, t) => `user ${u} turn ${t}: please remember item ${(t * 7) % 13} of my list ` + 'x'.repeat(t % 17);
const fakeModel = (u, t, nSeen) =>
  `assistant reply to user ${u} turn ${t}, saw ${nSeen} earlier messages, token ${createHash('sha256').update(`${u}/${t}/${nSeen}`).digest('hex').slice(0, 16)}`;
const users = Array.from({ length: USERS }, (_, i) => `user-${String(i).padStart(3, '0')}`);
const BASE = Date.UTC(2026, 0, 1);

async function pool(items, fn) {
  const queue = [...items];
  await Promise.all(Array.from({ length: WORKERS }, async () => {
    while (queue.length) await fn(queue.shift());
  }));
}

async function langgraphJs() {
  const { MongoClient } = await import('mongodb');
  const { MongoDBSaver } = await import('@langchain/langgraph-checkpoint-mongodb');
  const { StateGraph, MessagesAnnotation, START } = await import('@langchain/langgraph');
  const { AIMessage, HumanMessage } = await import('@langchain/core/messages');
  const client = new MongoClient(URI);
  const saver = new MongoDBSaver({ client, dbName: 'e2e_langgraph_js' });
  // the JS package creates its indexes only here (the Python one in its constructor); see results/ours-noindex/NOTE.md
  const setupErrors = await saver.setup();
  if (setupErrors.length) throw setupErrors[0];
  const graph = new StateGraph(MessagesAnnotation)
    .addNode('agent', async (state, config) => {
      const { thread_id: u, turn: t } = config.configurable;
      return { messages: [new AIMessage(fakeModel(u, t, state.messages.length - 1))] };
    })
    .addEdge(START, 'agent')
    .compile({ checkpointer: saver });
  await pool(users, async u => {
    for (let t = 0; t < TURNS; t++) {
      await graph.invoke({ messages: [new HumanMessage(userText(u, t))] }, { configurable: { thread_id: u, turn: t } });
    }
  });
  const out = {};
  for (const u of users) {
    const cfg = { configurable: { thread_id: u } };
    const st = await graph.getState(cfg);
    const history = [];
    for await (const c of saver.list(cfg)) {
      history.push([c.metadata?.step, c.metadata?.source, (c.checkpoint.channel_values?.messages || []).length]);
    }
    out[u] = { latest: st.values.messages.map(m => [m._getType(), m.content]), history };
  }
  await client.close();
  return out;
}

async function mastra() {
  const { MongoDBStore } = await import('./dist/index.js');
  const store = new MongoDBStore({ id: 'e2e', url: URI, dbName: 'e2e_mastra' });
  await store.init();
  const memory = await store.getStore('memory');
  await pool(users, async u => {
    const threadId = `thread-${u}`;
    await memory.saveThread({ thread: { id: threadId, resourceId: u, title: `chat of ${u}`, metadata: {}, createdAt: new Date(BASE), updatedAt: new Date(BASE) } });
    for (let t = 0; t < TURNS; t++) {
      const seen = (await memory.listMessages({ threadId, perPage: false })).messages.length;
      const msg = (role, text, k) => ({
        id: `${u}-${String(t).padStart(4, '0')}-${k}`, threadId, resourceId: u, role, type: 'v2',
        createdAt: new Date(BASE + t * 1000 + k), content: { format: 2, parts: [{ type: 'text', text }], content: text },
      });
      await memory.saveMessages({ messages: [msg('user', userText(u, t), 0), msg('assistant', fakeModel(u, t, seen), 1)] });
      await memory.saveResource({ resource: { id: u, workingMemory: `last turn ${t}`, metadata: { turn: t }, createdAt: new Date(BASE), updatedAt: new Date(BASE + t * 1000) } });
    }
  });
  const out = {};
  for (const u of users) {
    const threadId = `thread-${u}`;
    const all = (await memory.listMessages({ threadId, perPage: false })).messages;
    const last = (await memory.listMessages({ threadId, perPage: 4, page: 0, orderBy: { field: 'createdAt', direction: 'DESC' } })).messages;
    const res = await memory.getResourceById({ resourceId: u });
    const thread = await memory.getThreadById({ threadId });
    out[u] = {
      history: all.map(m => [m.id, m.role, m.content?.content]),
      latest4: last.map(m => [m.id, m.role, m.content?.content]),
      resource: [res?.workingMemory, res?.metadata],
      thread: [thread?.id, thread?.title, thread?.resourceId],
    };
  }
  await store.close();
  return out;
}

const [name, path] = process.argv.slice(2);
const t0 = Date.now();
const answers = await ({ 'langgraph-js': langgraphJs, mastra }[name])();
const seconds = Math.round((Date.now() - t0) / 100) / 10;
writeFileSync(path, JSON.stringify({ adapter: name, users: USERS, turns: TURNS, seconds, answers }));
console.log(`${name}: ${USERS} users x ${TURNS} turns in ${seconds} s`);
process.exit(0);
