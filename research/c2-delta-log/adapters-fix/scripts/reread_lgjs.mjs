// Re-read every LangGraph JS e2e thread (same reads as e2e_js.mjs) and compare with the e2e answers file: node reread_lgjs.mjs E2E.json OUT.json
import { readFileSync, writeFileSync } from 'node:fs';
const [refPath, outPath] = process.argv.slice(2);
const ref = JSON.parse(readFileSync(refPath, 'utf8'));
const { MongoClient } = await import('mongodb');
const { MongoDBSaver } = await import('@langchain/langgraph-checkpoint-mongodb');
const { StateGraph, MessagesAnnotation, START } = await import('@langchain/langgraph');
const client = new MongoClient('mongodb://localhost:27017/?directConnection=true');
const saver = new MongoDBSaver({ client, dbName: 'e2e_langgraph_js' });
const graph = new StateGraph(MessagesAnnotation).addNode('agent', async () => { throw new Error('read only'); })
  .addEdge(START, 'agent').compile({ checkpointer: saver });
const out = {};
for (const u of Object.keys(ref.answers).filter(k => !k.startsWith('_')).sort()) {
  const cfg = { configurable: { thread_id: u } };
  const st = await graph.getState(cfg);
  const history = [];
  for await (const c of saver.list(cfg)) history.push([c.metadata?.step, c.metadata?.source, (c.checkpoint.channel_values?.messages || []).length]);
  out[u] = { latest: st.values.messages.map(m => [m._getType(), m.content]), history };
}
await client.close();
const diff = Object.keys(ref.answers).filter(u => JSON.stringify(ref.answers[u]) !== JSON.stringify(out[u]));
writeFileSync(outPath, JSON.stringify({ adapter: 'langgraph-js', users: Object.keys(out).length, turns: ref.turns, seconds: 0, answers: out }));
console.log(`re-read ${Object.keys(out).length} threads; differences from the e2e answers of the same run: ${diff.length} ${diff.slice(0, 5)}`);
