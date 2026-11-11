# reproduces our server's getMore over the 48 MB wire limit: one customer's messages (2 KB text added by mutate.py),
# default batch size; mongod 7.0 answers the same query with batches capped at 16 MB
import pymongo, json
g = json.load(open("out/app_data_b02.gen.json"))
r = pymongo.MongoClient("mongodb://127.0.0.1:27018").app_data_b02.messaggi
cli = max((f"cli-{i:06d}" for i in g["sample_customers"]), key=lambda c: r.count_documents({"cliente": c}))
print("customer", cli, "messages", r.count_documents({"cliente": cli}))
for uri in ("mongodb://127.0.0.1:27018", "mongodb://127.0.0.1:27017"):
    c = pymongo.MongoClient(uri).app_data_b02.messaggi
    try:
        print(uri, "read", sum(1 for _ in c.find({"cliente": cli}).sort("ts", -1)), "documents")
    except Exception as e:
        print(uri, "ERROR", type(e).__name__, str(e)[:160])
