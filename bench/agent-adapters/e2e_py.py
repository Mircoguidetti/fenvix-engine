"""End-to-end check of one Python adapter against the target on localhost:27017: a tiny agent loop with a fake model
(no paid model) writes TURNS turns for each of USERS users, then reloads every user's latest state and history.

    e2e_py.py ADAPTER OUT.json     ADAPTER: langgraph | openai-agents | llamaindex | mem0

The answers (with random ids and timestamps removed) go to OUT.json; compare_e2e.py compares the files of mongod and ours.
Each adapter writes into its own database, e2e_<adapter>, so its size can be read afterwards (sizes.py).
Users run concurrently (WORKERS at a time), turns of one user in order, as an agent server would serve them.
"""
import asyncio
import hashlib
import json
import os
import sys
import time
from concurrent.futures import ThreadPoolExecutor

URI = os.environ.get("E2E_URI", "mongodb://localhost:27017/?directConnection=true")
USERS = int(os.environ.get("E2E_USERS", 50))
TURNS = int(os.environ.get("E2E_TURNS", 200))
WORKERS = int(os.environ.get("E2E_WORKERS", 10))


def user_text(u, t):
    return f"user {u} turn {t}: " + "please remember item %d of my list " % (t * 7 % 13) + "x" * (t % 17)


def fake_model(u, t, n_seen):
    """The fake model: deterministic, and it depends on how much history the adapter gave back."""
    h = hashlib.sha256(f"{u}/{t}/{n_seen}".encode()).hexdigest()[:16]
    return f"assistant reply to user {u} turn {t}, saw {n_seen} earlier messages, token {h}"


def users():
    return [f"user-{i:03d}" for i in range(USERS)]


# ---------------------------------------------------------------- LangGraph: MongoDBSaver + MongoDBStore
def run_langgraph():
    from langchain_core.messages import AIMessage, HumanMessage
    from langgraph.checkpoint.mongodb import MongoDBSaver
    from langgraph.graph import START, MessagesState, StateGraph
    from langgraph.store.mongodb import MongoDBStore
    from pymongo import MongoClient

    client = MongoClient(URI)
    db = "e2e_langgraph"
    saver = MongoDBSaver(client, db_name=db)
    store = MongoDBStore(client[db]["store"])

    def agent(state: MessagesState, config, *, store):
        u = config["configurable"]["thread_id"]
        t = config["configurable"]["turn"]
        reply = fake_model(u, t, len(state["messages"]) - 1)
        store.put(("profile", u), "last", {"turn": t, "reply": reply})
        store.put(("facts", u), f"t{t:04d}", {"text": user_text(u, t)})
        return {"messages": [AIMessage(content=reply)]}

    b = StateGraph(MessagesState)
    b.add_node("agent", agent)
    b.add_edge(START, "agent")
    graph = b.compile(checkpointer=saver, store=store)

    def one_user(u):
        for t in range(TURNS):
            graph.invoke({"messages": [HumanMessage(content=user_text(u, t))]},
                         {"configurable": {"thread_id": u, "turn": t}})

    with ThreadPoolExecutor(WORKERS) as ex:
        list(ex.map(one_user, users()))

    out = {}
    for u in users():
        cfg = {"configurable": {"thread_id": u}}
        st = graph.get_state(cfg)
        hist = list(saver.list(cfg))
        facts = store.search(("facts", u), limit=10_000)
        out[u] = {
            "latest": [[m.type, m.content] for m in st.values["messages"]],
            "history": [[c.metadata.get("step"), c.metadata.get("source"), len(c.checkpoint["channel_values"].get("messages", []))]
                        for c in hist],
            "profile": store.get(("profile", u), "last").value,
            "facts": sorted([f.key, f.value["text"]] for f in facts),
        }
    return out


# ---------------------------------------------------------------- OpenAI Agents SDK: MongoDBSession
def run_openai_agents():
    from agents import Agent, Runner
    from agents.extensions.memory import MongoDBSession
    from agents.testing import ScriptedModel
    from openai.types.responses import ResponseOutputMessage, ResponseOutputText
    from pymongo import AsyncMongoClient

    def text(content):
        return ResponseOutputMessage(id="1", type="message", role="assistant", status="completed",
                                     content=[ResponseOutputText(text=content, type="output_text", annotations=[], logprobs=[])])

    async def main():
        client = AsyncMongoClient(URI)
        sem = asyncio.Semaphore(WORKERS)

        async def one_user(u):
            async with sem:
                model = ScriptedModel()
                agent = Agent(name="assistant", model=model)
                session = MongoDBSession(u, client=client, database="e2e_openai_agents")
                for t in range(TURNS):
                    n_seen = len(await session.get_items())
                    model.enqueue([text(fake_model(u, t, n_seen))])
                    r = await Runner.run(agent, user_text(u, t), session=session)
                    assert r.final_output == fake_model(u, t, n_seen)

        await asyncio.gather(*(one_user(u) for u in users()))
        out = {}
        for u in users():
            s = MongoDBSession(u, client=client, database="e2e_openai_agents")
            items = await s.get_items()
            last = await s.get_items(limit=4)
            out[u] = {"history": [canon_item(i) for i in items], "latest4": [canon_item(i) for i in last]}
        await client.close()
        return out

    return asyncio.run(main())


def canon_item(i):
    i = dict(i)
    i.pop("id", None)
    if isinstance(i.get("content"), list):
        i["content"] = [{k: v for k, v in c.items() if k in ("type", "text")} for c in i["content"]]
    return i


# ---------------------------------------------------------------- LlamaIndex: MongoChatStore
def run_llamaindex():
    from llama_index.core.llms import ChatMessage
    from llama_index.storage.chat_store.mongo import MongoChatStore

    store = MongoChatStore(mongo_uri=URI, db_name="e2e_llamaindex", collection_name="sessions")

    def one_user(u):
        for t in range(TURNS):
            seen = len(store.get_messages(u))
            store.add_message(u, ChatMessage(role="user", content=user_text(u, t)))
            store.add_message(u, ChatMessage(role="assistant", content=fake_model(u, t, seen)))

    with ThreadPoolExecutor(WORKERS) as ex:
        list(ex.map(one_user, users()))
    out = {}
    for u in users():
        msgs = store.get_messages(u)
        out[u] = {"history": [[m.role.value, m.content] for m in msgs], "latest": [msgs[-1].role.value, msgs[-1].content]}
    out["_keys"] = sorted(store.get_keys())
    return out


# ---------------------------------------------------------------- Mem0: the mongodb vector store
def run_mem0():
    import logging

    from mem0.vector_stores.mongodb import MongoDB

    errors = []

    class Catch(logging.Handler):  # the adapter logs and swallows driver errors: collect them
        def emit(self, rec):
            if rec.levelno >= logging.ERROR:
                errors.append(rec.getMessage()[:200])

    logging.getLogger("mem0.vector_stores.mongodb").addHandler(Catch())
    dims = 16
    vs = MongoDB(db_name="e2e_mem0", collection_name="memories", embedding_model_dims=dims, mongo_uri=URI)
    setup_errors = list(errors)
    errors.clear()
    # On community mongod the adapter's setup fails ($listSearchIndexes is Atlas-only) and leaves no collection:
    # every later call would crash. Give it the plain collection so the parts that can run, run (noted in the output).
    collection_was_missing = vs.collection is None
    if collection_was_missing:
        vs.collection = vs.db["memories"]
        vs.index_name = "memories_vector_index"

    def embed(s):
        h = hashlib.sha256(s.encode()).digest()
        return [b / 255.0 for b in h[:dims]]

    def one_user(u):
        for t in range(TURNS):
            seen = len(vs.list(filters={"user_id": u}, top_k=10_000)[0])
            mem = f"{user_text(u, t)} | {fake_model(u, t, seen)}"
            vs.insert([embed(mem)], [{"user_id": u, "data": mem, "turn": t}], [f"{u}-{t:04d}"])

    with ThreadPoolExecutor(WORKERS) as ex:
        list(ex.map(one_user, users()))
    out = {}
    for u in users():
        allm = vs.list(filters={"user_id": u}, top_k=10_000)[0]
        latest = vs.get(f"{u}-{TURNS - 1:04d}")
        out[u] = {"history": sorted([m.id, m.payload["turn"], m.payload["data"]] for m in allm),
                  "latest": [latest.id, latest.payload] if latest else None}
    try:
        search = vs.search("anything", embed("anything"), top_k=5, filters={"user_id": "user-000"})
        out["_search_by_meaning"] = {"results": len(search), "errors": [e[:120] for e in errors][:3]}
    except Exception as e:  # search() calls $listSearchIndexes outside its own try
        out["_search_by_meaning"] = {"results": None, "errors": [f"raised {type(e).__name__}: {str(e)[:100]}"]}
    out["_setup"] = {"errors": [e[:90] for e in setup_errors], "collection_missing_after_setup": collection_was_missing}
    return out


if __name__ == "__main__":
    name, path = sys.argv[1], sys.argv[2]
    fn = {"langgraph": run_langgraph, "openai-agents": run_openai_agents, "llamaindex": run_llamaindex, "mem0": run_mem0}[name]
    t0 = time.time()
    res = fn()
    secs = time.time() - t0
    with open(path, "w") as f:
        json.dump({"adapter": name, "users": USERS, "turns": TURNS, "seconds": round(secs, 1), "answers": res}, f, sort_keys=True)
    print(f"{name}: {USERS} users x {TURNS} turns in {secs:.0f} s")
