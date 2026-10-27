"""Cases for the Atlas Search operators added in round 10 (near, geoWithin, geoShape, moreLikeThis, queryString,
embeddedDocument, facet, highlight). mongo:7.0 has no Atlas Search, so every expectation is derived from the documented
Atlas semantics (formulas below are written independently of the server). The cases are written to search_ops_cases.json
and run by search_ops_test.py (pymongo) and node/search_ops_test.js (Node driver)."""
import json, math

R = 6371008.7714
def hav(lng1, lat1, lng2, lat2):
    p1, p2 = math.radians(lat1), math.radians(lat2)
    h = math.sin((p2 - p1) / 2) ** 2 + math.cos(p1) * math.cos(p2) * math.sin(math.radians(lng2 - lng1) / 2) ** 2
    return 2 * R * math.asin(min(1, math.sqrt(h)))

def pt(lng, lat): return {"type": "Point", "coordinates": [lng, lat]}
def box(lng, lat, h=0.005):
    return {"type": "Polygon", "coordinates": [[[lng - h, lat - h], [lng + h, lat - h], [lng + h, lat + h], [lng - h, lat + h], [lng - h, lat - h]]]}
def D(s): return {"$date": s + "T00:00:00Z"}

PLACES = [
    (1, "Central Park", "park", 4.5, "2000-06-01", (-73.9654, 40.7829), "A large public park in Manhattan. It has many trees and lakes. Visitors love the park.", ["green", "city"],
     [{"name": "boat rental", "price": 20}, {"name": "bike rental", "price": 15}]),
    (2, "Prospect Park", "park", 4.7, "2001-06-01", (-73.9690, 40.6602), "A public park in Brooklyn. It has a lake and a zoo.", ["green"], [{"name": "zoo ticket", "price": 10}]),
    (3, "Times Square", "plaza", 3.9, "1999-06-01", (-73.9855, 40.7580), "A busy commercial intersection. Many theaters and shops.", ["city", "tourist"],
     [{"name": "souvenir", "price": 5}, {"name": "theater ticket", "price": 100}]),
    (4, "Statue of Liberty", "monument", 4.8, "1886-10-28", (-74.0445, 40.6892), "A colossal statue on Liberty Island. Ferry tickets are needed.", [], [{"name": "ferry ticket", "price": 25}]),
    (5, "Empire State Building", "monument", 4.6, "1931-05-01", (-73.9857, 40.7484), "An Art Deco skyscraper in Midtown Manhattan.", ["city"], []),
    (6, "No Location", "misc", 2.0, None, None, "nothing here", [], []),
]
ZONES = {1: box(-73.9654, 40.7829), 2: box(-73.9690, 40.6602), 3: box(-73.9855, 40.7580), 4: pt(-74.0445, 40.6892),
         5: {"type": "LineString", "coordinates": [[-73.9867, 40.7474], [-73.9847, 40.7494]]}}
DOCS = []
for (i, name, kind, rating, opened, loc, body, tags, items) in PLACES:
    d = {"_id": i, "name": name, "kind": kind, "rating": rating, "body": body, "tags": tags, "items": items}
    if opened: d["opened"] = D(opened)
    if loc: d["loc"] = pt(*loc)
    if i in ZONES: d["zone"] = ZONES[i]
    DOCS.append(d)
LOC = {p[0]: p[5] for p in PLACES if p[5]}

cases = []
def case(name, pipeline, expect, kind="ids", **kw):
    cases.append({"name": name, "pipeline": pipeline, "kind": kind, "expect": expect, **kw})

S = lambda op, *rest: [{"$search": op}] + list(rest)
IDS = {"$project": {"_id": 1}}
SCORE = {"$project": {"s": {"$meta": "searchScore"}}}

# ---------------------------------------------------------------- near
def score_order(dists, pivot):
    sc = {i: pivot / (pivot + d) for i, d in dists.items()}
    return sc, [i for i, _ in sorted(sc.items(), key=lambda kv: (-kv[1], kv[0]))]
ratings = {p[0]: abs(p[3] - 4.7) for p in PLACES}
sc, order = score_order(ratings, 0.5)
case("near number order", S({"near": {"path": "rating", "origin": 4.7, "pivot": 0.5}}, IDS), order, "ordered")
case("near number scores", S({"near": {"path": "rating", "origin": 4.7, "pivot": 0.5}}, {"$project": {"s": {"$meta": "searchScore"}}}), {str(i): v for i, v in sc.items()}, "scoresById")
case("near integer origin", S({"near": {"path": "rating", "origin": 5, "pivot": 1}}, IDS), score_order({p[0]: abs(p[3] - 5) for p in PLACES}, 1)[1], "ordered")
od = {p[0]: abs((_d - 946684800000)) for p in PLACES if p[4] for _d in [int(__import__("calendar").timegm(__import__("time").strptime(p[4], "%Y-%m-%d"))) * 1000]}
sc, order = score_order(od, 31536000000.0)
case("near date", S({"near": {"path": "opened", "origin": D("2000-01-01"), "pivot": 31536000000}}, IDS), order, "ordered")
case("near date scores", S({"near": {"path": "opened", "origin": D("2000-01-01"), "pivot": 31536000000}}, SCORE), {str(i): v for i, v in sc.items()}, "scoresById")
ts = LOC[3]
gd = {i: hav(ts[0], ts[1], *l) for i, l in LOC.items()}
sc, order = score_order(gd, 5000.0)
case("near geo", S({"near": {"path": "loc", "origin": pt(*ts), "pivot": 5000}}, IDS), order, "ordered")
case("near geo scores", S({"near": {"path": "loc", "origin": pt(*ts), "pivot": 5000}}, SCORE), {str(i): v for i, v in sc.items()}, "scoresById")
case("near geo limit", S({"near": {"path": "loc", "origin": pt(*ts), "pivot": 5000}}, {"$limit": 2}, IDS), order[:2], "ordered")
case("near wrong type", S({"near": {"path": "name", "origin": 5, "pivot": 1}}, IDS), [])
case("near boost", S({"near": {"path": "rating", "origin": 4.7, "pivot": 0.5, "score": {"boost": {"value": 3}}}}, SCORE), {str(i): 3 * v for i, v in score_order(ratings, 0.5)[0].items()}, "scoresById")
case("near in compound filter", S({"compound": {"must": [{"near": {"path": "rating", "origin": 4.7, "pivot": 0.5}}], "filter": [{"equals": {"path": "kind", "value": "monument"}}]}}, IDS), [4, 5], "ordered")
case("near no pivot", S({"near": {"path": "rating", "origin": 4.7}}, IDS), None, "error")
case("near zero pivot", S({"near": {"path": "rating", "origin": 4.7, "pivot": 0}}, IDS), None, "error")
case("near no origin", S({"near": {"path": "rating", "pivot": 1}}, IDS), None, "error")
case("near bad origin", S({"near": {"path": "rating", "origin": "x", "pivot": 1}}, IDS), None, "error")

# ---------------------------------------------------------------- geoWithin
inside = lambda c, r: sorted(i for i, l in LOC.items() if hav(c[0], c[1], *l) <= r)
case("geoWithin circle", S({"geoWithin": {"path": "loc", "circle": {"center": pt(*ts), "radius": 2000}}}, IDS), inside(ts, 2000))
case("geoWithin circle big", S({"geoWithin": {"path": "loc", "circle": {"center": pt(*ts), "radius": 30000}}}, IDS), inside(ts, 30000))
case("geoWithin circle small", S({"geoWithin": {"path": "loc", "circle": {"center": pt(*ts), "radius": 10}}}, IDS), [3])
rect = (-74.0, 40.70, -73.97, 40.76)
inbox = sorted(i for i, l in LOC.items() if rect[0] <= l[0] <= rect[2] and rect[1] <= l[1] <= rect[3])
case("geoWithin box", S({"geoWithin": {"path": "loc", "box": {"bottomLeft": pt(rect[0], rect[1]), "topRight": pt(rect[2], rect[3])}}}, IDS), inbox)
poly = {"type": "Polygon", "coordinates": [[[rect[0], rect[1]], [rect[2], rect[1]], [rect[2], rect[3]], [rect[0], rect[3]], [rect[0], rect[1]]]]}
case("geoWithin polygon", S({"geoWithin": {"path": "loc", "geometry": poly}}, IDS), inbox)
case("geoWithin multipolygon", S({"geoWithin": {"path": "loc", "geometry": {"type": "MultiPolygon", "coordinates": [poly["coordinates"], box(-73.9690, 40.6602)["coordinates"]]}}}, IDS), sorted(inbox + [2]))
case("geoWithin in compound", S({"compound": {"filter": [{"geoWithin": {"path": "loc", "circle": {"center": pt(*ts), "radius": 30000}}}], "must": [{"text": {"path": "body", "query": "park"}}]}}, IDS), [1, 2], "ids")
case("geoWithin ignores non points", S({"geoWithin": {"path": "zone", "circle": {"center": pt(*ts), "radius": 30000}}}, IDS), [4])
case("geoWithin two shapes", S({"geoWithin": {"path": "loc", "box": {"bottomLeft": pt(0, 0), "topRight": pt(1, 1)}, "circle": {"center": pt(0, 0), "radius": 1}}}, IDS), None, "error")
case("geoWithin point geometry", S({"geoWithin": {"path": "loc", "geometry": pt(0, 0)}}, IDS), None, "error")
case("geoWithin none", S({"geoWithin": {"path": "loc"}}, IDS), None, "error")

# ---------------------------------------------------------------- geoShape
def qbox(lng, lat, h): return box(lng, lat, h)
qs = box(-73.9654, 40.7829, 0.002)       # inside the Central Park zone
big = box(-73.9654, 40.7829, 0.015)      # contains the Central Park zone
away = box(-73.0, 40.0, 0.01)
case("geoShape intersects small", S({"geoShape": {"path": "zone", "relation": "intersects", "geometry": qs}}, IDS), [1])
case("geoShape intersects big", S({"geoShape": {"path": "zone", "relation": "intersects", "geometry": big}}, IDS), [1])
case("geoShape within big", S({"geoShape": {"path": "zone", "relation": "within", "geometry": big}}, IDS), [1])
case("geoShape within small", S({"geoShape": {"path": "zone", "relation": "within", "geometry": qs}}, IDS), [])
case("geoShape contains small", S({"geoShape": {"path": "zone", "relation": "contains", "geometry": qs}}, IDS), [1])
case("geoShape contains big", S({"geoShape": {"path": "zone", "relation": "contains", "geometry": big}}, IDS), [])
case("geoShape disjoint", S({"geoShape": {"path": "zone", "relation": "disjoint", "geometry": big}}, IDS), [2, 3, 4, 5])
case("geoShape disjoint away", S({"geoShape": {"path": "zone", "relation": "disjoint", "geometry": away}}, IDS), [1, 2, 3, 4, 5])
case("geoShape intersects point zone", S({"geoShape": {"path": "zone", "relation": "intersects", "geometry": box(-74.0445, 40.6892, 0.01)}}, IDS), [4])
case("geoShape intersects line zone", S({"geoShape": {"path": "zone", "relation": "intersects", "geometry": box(-73.9857, 40.7484, 0.002)}}, IDS), [5])
case("geoShape within covers point", S({"geoShape": {"path": "zone", "relation": "within", "geometry": box(-74.0445, 40.6892, 0.01)}}, IDS), [4])
case("geoShape query point intersects", S({"geoShape": {"path": "zone", "relation": "intersects", "geometry": pt(-73.9655, 40.7830)}}, IDS), [1])
case("geoShape query point contains", S({"geoShape": {"path": "zone", "relation": "contains", "geometry": pt(-73.9655, 40.7830)}}, IDS), [1])
case("geoShape within point query error", S({"geoShape": {"path": "zone", "relation": "within", "geometry": pt(0, 0)}}, IDS), None, "error")
case("geoShape bad relation", S({"geoShape": {"path": "zone", "relation": "touches", "geometry": big}}, IDS), None, "error")
case("geoShape no geometry", S({"geoShape": {"path": "zone", "relation": "intersects"}}, IDS), None, "error")
case("geoShape line query", S({"geoShape": {"path": "zone", "relation": "intersects", "geometry": {"type": "LineString", "coordinates": [[-73.97, 40.78], [-73.96, 40.785]]}}}, IDS), [1])
case("geoShape over points", S({"geoShape": {"path": "loc", "relation": "within", "geometry": poly}}, IDS), inbox)

# ---------------------------------------------------------------- queryString
def qs_case(name, q, want, path="body", **kw):
    if want is None:
        case("queryString " + name, S({"queryString": {"defaultPath": path, "query": q}}, IDS), None, "error")
    else:
        case("queryString " + name, S({"queryString": {"defaultPath": path, "query": q}}, IDS), want, **kw)
qs_case("term", "park", [1, 2])
qs_case("and", "park AND lake", [2])
qs_case("and symbol", "park && lake", [2])
qs_case("or", "park OR statue", [1, 2, 4])
qs_case("or symbol", "statue || skyscraper", [4, 5])
qs_case("default or", "statue theaters", [3, 4])
qs_case("minus", "park -brooklyn", [1])
qs_case("plus", "+park +manhattan", [1])
qs_case("plus only", "+park lake", [1, 2])
qs_case("not alone", "NOT park", [])
qs_case("not bang", "!park", [])
qs_case("park not", "park NOT brooklyn", [1])
qs_case("group", "(park OR statue) AND ferry", [4])
qs_case("nested group", "((park OR statue) AND (ferry OR zoo))", [2, 4])
qs_case("field", "name:central", [1])
qs_case("field phrase", 'name:"times square"', [3])
qs_case("phrase", '"public park"', [1, 2])
qs_case("phrase miss", '"park public"', [])
qs_case("phrase slop", '"park manhattan"~3', [1])
qs_case("phrase slop zero", '"park manhattan"', [])
qs_case("wildcard star", "man*", [1, 3, 5])
qs_case("wildcard question", "tick?ts", [4])
qs_case("fuzzy", "lak~1", [2])
qs_case("fuzzy default", "lake~", [1, 2, 4])
qs_case("field group", "name:(central OR prospect)", [1, 2])
qs_case("numeric range", "rating:[4.6 TO 4.8]", [2, 4, 5])
qs_case("numeric range exclusive", "rating:{4.6 TO 4.8}", [2])
qs_case("numeric range open", "rating:[4.7 TO *]", [2, 4])
qs_case("string range", "name:[a TO d]", [1, 5])
qs_case("boost", "park^2 OR statue", [1, 2, 4])
qs_case("case insensitive", "PARK", [1, 2])
qs_case("escaped", "tick\\?ts", [])
qs_case("match all", "*:*", [1, 2, 3, 4, 5, 6])
qs_case("and or precedence", "park AND lake OR statue", [2])
qs_case("path name", "central OR statue", [1, 4], path="name")
qs_case("syntax dangling and", "park AND", None)
qs_case("syntax open paren", "(park", None)
qs_case("syntax close paren", "park)", None)
qs_case("syntax unterminated phrase", '"park', None)
qs_case("syntax bad range", "rating:[1 4]", None)
case("queryString no path", S({"queryString": {"query": "park"}}, IDS), None, "error")
case("queryString no query", S({"queryString": {"defaultPath": "body"}}, IDS), None, "error")
case("queryString scores positive", S({"queryString": {"defaultPath": "body", "query": "park"}}, {"$project": {"ok": {"$gt": [{"$meta": "searchScore"}, 0]}}}), {"1": {"ok": True}, "2": {"ok": True}}, "docsById")

# ---------------------------------------------------------------- moreLikeThis
case("mlt brooklyn zoo", S({"moreLikeThis": {"like": {"body": "Brooklyn zoo"}}}, IDS), [2])
case("mlt name", S({"moreLikeThis": {"like": {"name": "Central Park"}}}, IDS), [1, 2])
case("mlt array", S({"moreLikeThis": {"like": [{"name": "Statue"}, {"name": "Times"}]}}, IDS), [3, 4])
case("mlt unknown words", S({"moreLikeThis": {"like": {"body": "zzzzz qqqqq"}}}, IDS), [])
case("mlt wrong field", S({"moreLikeThis": {"like": {"nonexistent": "park"}}}, IDS), [])
case("mlt source document", S({"moreLikeThis": {"like": {"name": "Empire State Building", "body": "An Art Deco skyscraper in Midtown Manhattan."}}}, IDS), [1, 2, 5])
case("mlt excluding source", S({"compound": {"must": [{"moreLikeThis": {"like": {"name": "Central Park"}}}], "mustNot": [{"equals": {"path": "_id", "value": 1}}]}}, IDS), [2])
case("mlt not a document", S({"moreLikeThis": {"like": "park"}}, IDS), None, "error")
case("mlt missing like", S({"moreLikeThis": {}}, IDS), None, "error")
case("mlt scores ordered", S({"moreLikeThis": {"like": {"body": "park lake zoo Brooklyn"}}}, IDS), [2, 1], "ordered")

# ---------------------------------------------------------------- embeddedDocument
case("embedded text", S({"embeddedDocument": {"path": "items", "operator": {"text": {"path": "items.name", "query": "ticket"}}}}, IDS), [2, 3, 4])
case("embedded same element", S({"embeddedDocument": {"path": "items", "operator": {"compound": {"must": [{"text": {"path": "items.name", "query": "ticket"}}, {"range": {"path": "items.price", "lte": 20}}]}}}}, IDS), [2])
case("embedded different elements do not combine", S({"embeddedDocument": {"path": "items", "operator": {"compound": {"must": [{"text": {"path": "items.name", "query": "souvenir"}}, {"range": {"path": "items.price", "gte": 100}}]}}}}, IDS), [])
case("embedded range", S({"embeddedDocument": {"path": "items", "operator": {"range": {"path": "items.price", "gt": 50}}}}, IDS), [3])
case("embedded no array", S({"embeddedDocument": {"path": "items", "operator": {"text": {"path": "items.name", "query": "zzz"}}}}, IDS), [])
agg = lambda a: S({"embeddedDocument": {"path": "items", "operator": {"range": {"path": "items.price", "gte": 0}}, "score": {"embedded": {"aggregate": a}}}}, SCORE) if a else S({"embeddedDocument": {"path": "items", "operator": {"range": {"path": "items.price", "gte": 0}}}}, SCORE)
case("embedded score default sum", agg(None), {"1": 2.0, "2": 1.0, "3": 2.0, "4": 1.0}, "scoresById")
case("embedded score sum", agg("sum"), {"1": 2.0, "2": 1.0, "3": 2.0, "4": 1.0}, "scoresById")
case("embedded score maximum", agg("maximum"), {"1": 1.0, "2": 1.0, "3": 1.0, "4": 1.0}, "scoresById")
case("embedded score minimum", agg("minimum"), {"1": 1.0, "2": 1.0, "3": 1.0, "4": 1.0}, "scoresById")
case("embedded score mean", agg("mean"), {"1": 1.0, "2": 1.0, "3": 1.0, "4": 1.0}, "scoresById")
case("embedded bad aggregate", agg("median"), None, "error")
case("embedded no operator", S({"embeddedDocument": {"path": "items"}}, IDS), None, "error")
case("embedded no path", S({"embeddedDocument": {"operator": {"range": {"path": "items.price", "gt": 1}}}}, IDS), None, "error")

# ---------------------------------------------------------------- facet
M = lambda facets, **kw: [{"$searchMeta": dict({"facet": facets}, **kw)}]
kind_facet = {"facets": {"k": {"type": "string", "path": "kind"}}}
case("facet string", M(kind_facet), [{"count": {"lowerBound": 6}, "facet": {"k": {"buckets": [{"_id": "monument", "count": 2}, {"_id": "park", "count": 2}, {"_id": "misc", "count": 1}, {"_id": "plaza", "count": 1}]}}}], "exact")
case("facet string numBuckets", M({"facets": {"k": {"type": "string", "path": "kind", "numBuckets": 2}}}), [{"count": {"lowerBound": 6}, "facet": {"k": {"buckets": [{"_id": "monument", "count": 2}, {"_id": "park", "count": 2}]}}}], "exact")
case("facet string array values", M({"facets": {"t": {"type": "string", "path": "tags"}}}), [{"count": {"lowerBound": 6}, "facet": {"t": {"buckets": [{"_id": "city", "count": 3}, {"_id": "green", "count": 2}, {"_id": "tourist", "count": 1}]}}}], "exact")
case("facet number", M({"facets": {"r": {"type": "number", "path": "rating", "boundaries": [0, 3, 4, 5], "default": "other"}}}), [{"count": {"lowerBound": 6}, "facet": {"r": {"buckets": [{"_id": 0, "count": 1}, {"_id": 3, "count": 1}, {"_id": 4, "count": 4}, {"_id": "other", "count": 0}]}}}], "exact")
case("facet number default", M({"facets": {"r": {"type": "number", "path": "rating", "boundaries": [4, 4.6, 5], "default": "other"}}}), [{"count": {"lowerBound": 6}, "facet": {"r": {"buckets": [{"_id": 4, "count": 1}, {"_id": 4.6, "count": 3}, {"_id": "other", "count": 2}]}}}], "exact")
case("facet number no default", M({"facets": {"r": {"type": "number", "path": "rating", "boundaries": [4, 4.6, 5]}}}), [{"count": {"lowerBound": 6}, "facet": {"r": {"buckets": [{"_id": 4, "count": 1}, {"_id": 4.6, "count": 3}]}}}], "exact")
case("facet date", M({"facets": {"o": {"type": "date", "path": "opened", "boundaries": [D("1990-01-01"), D("2000-01-01"), D("2001-01-01"), D("2002-01-01")], "default": "old"}}}), [{"count": {"lowerBound": 6}, "facet": {"o": {"buckets": [{"_id": D("1990-01-01"), "count": 1}, {"_id": D("2000-01-01"), "count": 1}, {"_id": D("2001-01-01"), "count": 1}, {"_id": "old", "count": 2}]}}}], "exact")
case("facet with operator", M({"operator": {"text": {"path": "body", "query": "park"}}, "facets": {"k": {"type": "string", "path": "kind"}}}), [{"count": {"lowerBound": 2}, "facet": {"k": {"buckets": [{"_id": "park", "count": 2}]}}}], "exact")
case("facet several", M({"facets": {"k": {"type": "string", "path": "kind", "numBuckets": 1}, "r": {"type": "number", "path": "rating", "boundaries": [0, 5]}}}), [{"count": {"lowerBound": 6}, "facet": {"k": {"buckets": [{"_id": "monument", "count": 2}]}, "r": {"buckets": [{"_id": 0, "count": 6}]}}}], "exact")
case("facet count total", M(kind_facet, count={"type": "total"}), [{"count": {"total": 6}, "facet": {"k": {"buckets": [{"_id": "monument", "count": 2}, {"_id": "park", "count": 2}, {"_id": "misc", "count": 1}, {"_id": "plaza", "count": 1}]}}}], "exact")
case("facet via $search and SEARCH_META", [{"$search": {"facet": {"operator": {"text": {"path": "body", "query": "park"}}, "facets": {"k": {"type": "string", "path": "kind"}}}}}, {"$limit": 1}, {"$project": {"_id": 0, "meta": "$$SEARCH_META"}}],
     [{"meta": {"count": {"lowerBound": 2}, "facet": {"k": {"buckets": [{"_id": "park", "count": 2}]}}}}], "exact")
case("facet $search documents", [{"$search": {"facet": {"operator": {"text": {"path": "body", "query": "park"}}, "facets": {"k": {"type": "string", "path": "kind"}}}}}, IDS], [1, 2], "ordered")
case("facet no facets", M({"facets": {}}), None, "error")
case("facet missing facets", M({"operator": {"text": {"path": "body", "query": "park"}}}), None, "error")
case("facet unsorted boundaries", M({"facets": {"r": {"type": "number", "path": "rating", "boundaries": [5, 1]}}}), None, "error")
case("facet one boundary", M({"facets": {"r": {"type": "number", "path": "rating", "boundaries": [5]}}}), None, "error")
case("facet bad type", M({"facets": {"r": {"type": "bool", "path": "rating"}}}), None, "error")
case("facet no path", M({"facets": {"r": {"type": "string"}}}), None, "error")
case("facet numBuckets zero", M({"facets": {"k": {"type": "string", "path": "kind", "numBuckets": 0}}}), None, "error")

# ---------------------------------------------------------------- highlight
H = lambda op, hl, *rest: S(dict(op, highlight=hl), *rest)
HL = {"$project": {"_id": 1, "hl": {"$meta": "searchHighlights"}}}
park = {"text": {"path": "body", "query": "park"}}
case("highlight passages", H(park, {"path": "body"}, {"$sort": {"_id": 1}}, HL), [
    {"_id": 1, "hl": [{"path": "body", "texts": [{"value": "A large public ", "type": "text"}, {"value": "park", "type": "hit"}, {"value": " in Manhattan.", "type": "text"}]},
                      {"path": "body", "texts": [{"value": "Visitors love the ", "type": "text"}, {"value": "park", "type": "hit"}, {"value": ".", "type": "text"}]}]},
    {"_id": 2, "hl": [{"path": "body", "texts": [{"value": "A public ", "type": "text"}, {"value": "park", "type": "hit"}, {"value": " in Brooklyn.", "type": "text"}]}]}], "docsNoScore")
case("highlight maxNumPassages", H(park, {"path": "body", "maxNumPassages": 1}, {"$sort": {"_id": 1}}, HL), [
    {"_id": 1, "hl": [{"path": "body", "texts": [{"value": "A large public ", "type": "text"}, {"value": "park", "type": "hit"}, {"value": " in Manhattan.", "type": "text"}]}]},
    {"_id": 2, "hl": [{"path": "body", "texts": [{"value": "A public ", "type": "text"}, {"value": "park", "type": "hit"}, {"value": " in Brooklyn.", "type": "text"}]}]}], "docsNoScore")
case("highlight other path", H(park, {"path": "name"}, {"$sort": {"_id": 1}}, HL), [{"_id": 1, "hl": []}, {"_id": 2, "hl": []}], "docsNoScore")
case("highlight two paths", H({"text": {"path": ["name", "body"], "query": "park"}}, {"path": ["name", "body"]}, {"$match": {"_id": 2}}, HL), [
    {"_id": 2, "hl": [{"path": "name", "texts": [{"value": "Prospect ", "type": "text"}, {"value": "Park", "type": "hit"}]},
                      {"path": "body", "texts": [{"value": "A public ", "type": "text"}, {"value": "park", "type": "hit"}, {"value": " in Brooklyn.", "type": "text"}]}]}], "docsNoScore")
case("highlight wildcard path", H({"text": {"path": {"wildcard": "*"}, "query": "brooklyn"}}, {"path": {"wildcard": "*"}}, HL), [
    {"_id": 2, "hl": [{"path": "body", "texts": [{"value": "A public park in ", "type": "text"}, {"value": "Brooklyn", "type": "hit"}, {"value": ".", "type": "text"}]}]}], "docsNoScore")
case("highlight multiple hits", H({"text": {"path": "body", "query": "lake zoo"}}, {"path": "body"}, HL), [
    {"_id": 2, "hl": [{"path": "body", "texts": [{"value": "It has a ", "type": "text"}, {"value": "lake", "type": "hit"}, {"value": " and a ", "type": "text"}, {"value": "zoo", "type": "hit"}, {"value": ".", "type": "text"}]}]}], "docsNoScore")
case("highlight phrase", H({"phrase": {"path": "body", "query": "large public"}}, {"path": "body"}, HL), [
    {"_id": 1, "hl": [{"path": "body", "texts": [{"value": "A ", "type": "text"}, {"value": "large", "type": "hit"}, {"value": " ", "type": "text"}, {"value": "public", "type": "hit"}, {"value": " park in Manhattan.", "type": "text"}]}]}], "docsNoScore")
case("highlight in compound", H({"compound": {"must": [park], "should": [{"text": {"path": "body", "query": "zoo"}}]}}, {"path": "body"}, {"$match": {"_id": 2}}, HL), [
    {"_id": 2, "hl": [{"path": "body", "texts": [{"value": "A public ", "type": "text"}, {"value": "park", "type": "hit"}, {"value": " in Brooklyn.", "type": "text"}]},
                      {"path": "body", "texts": [{"value": "It has a lake and a ", "type": "text"}, {"value": "zoo", "type": "hit"}, {"value": ".", "type": "text"}]}]}], "docsNoScore")
case("highlight queryString", H({"queryString": {"defaultPath": "body", "query": "ferry"}}, {"path": "body"}, HL), [
    {"_id": 4, "hl": [{"path": "body", "texts": [{"value": "Ferry", "type": "hit"}, {"value": " tickets are needed.", "type": "text"}]}]}], "docsNoScore")
case("highlight passage count", H(park, {"path": "body"}, {"$match": {"_id": 1}}, {"$project": {"_id": 0, "n": {"$size": {"$meta": "searchHighlights"}}}}), [{"n": 2}], "exact")
case("highlight meta absent", S(park, {"$project": {"h": {"$meta": "searchHighlights"}}}), None, "error")
case("highlight no path", H(park, {}, HL), None, "error")
case("highlight bad maxNumPassages", H(park, {"path": "body", "maxNumPassages": 0}, HL), None, "error")
case("highlight in searchMeta", [{"$searchMeta": dict(park, highlight={"path": "body"})}], None, "error")
cases = [c for c in cases if c]
json.dump({"docs": DOCS, "cases": cases}, open("search_ops_cases.json", "w"), indent=1)
print(len(cases), "cases")
