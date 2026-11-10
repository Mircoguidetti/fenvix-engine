"""Re-read every LangGraph e2e thread (same reads as e2e_py.py run_langgraph) and compare with the e2e answers file.
    reread_lg.py E2E.json OUT.json"""
import json, sys
from langchain_core.messages import AIMessage
from langgraph.checkpoint.mongodb import MongoDBSaver
from langgraph.graph import START, MessagesState, StateGraph
from langgraph.store.mongodb import MongoDBStore
from pymongo import MongoClient
ref = json.load(open(sys.argv[1]))
client = MongoClient("mongodb://localhost:27017/?directConnection=true")
db = "e2e_langgraph"
saver = MongoDBSaver(client, db_name=db)
store = MongoDBStore(client[db]["store"])
def agent(state: MessagesState, config, *, store):
    raise RuntimeError("read only")
b = StateGraph(MessagesState); b.add_node("agent", agent); b.add_edge(START, "agent")
graph = b.compile(checkpointer=saver, store=store)
out = {}
for u in sorted(k for k in ref["answers"] if not k.startswith("_")):
    cfg = {"configurable": {"thread_id": u}}
    st = graph.get_state(cfg)
    hist = list(saver.list(cfg))
    facts = store.search(("facts", u), limit=10_000)
    out[u] = {"latest": [[m.type, m.content] for m in st.values["messages"]],
              "history": [[c.metadata.get("step"), c.metadata.get("source"), len(c.checkpoint["channel_values"].get("messages", []))] for c in hist],
              "profile": store.get(("profile", u), "last").value,
              "facts": sorted([f.key, f.value["text"]] for f in facts)}
out = json.loads(json.dumps(out))
diff = [u for u in ref["answers"] if ref["answers"][u] != out.get(u)]
json.dump({"adapter": "langgraph", "users": len(out), "turns": ref["turns"], "seconds": 0, "answers": out}, open(sys.argv[2], "w"), sort_keys=True)
print(f"re-read {len(out)} threads; differences from the e2e answers of the same run: {len(diff)} {diff[:5]}")
