#!/usr/bin/env python3
"""Regenerate mongo-server/src/builtin_roles.json from a real mongod --auth (auth_env.sh start; root/rootpw on 27030)."""
import json, sys
from pymongo import MongoClient
c = MongoClient("mongodb://root:rootpw@localhost:27030/?authSource=admin")
out = {}
for db, roles in (("tpl", c.tpl.command("rolesInfo", 1, showBuiltinRoles=True)["roles"]), ("admin", c.admin.command("rolesInfo", 1, showBuiltinRoles=True)["roles"])):
    for r in roles:
        x = c[db].command("rolesInfo", {"role": r["role"], "db": db}, showPrivileges=True)["roles"][0]
        out[f"{db}.{r['role']}"] = {k: x.get(k, []) for k in ("privileges", "inheritedPrivileges", "roles", "inheritedRoles")}
json.dump(out, open(sys.argv[1] if len(sys.argv) > 1 else "../../mongo-server/src/builtin_roles.json", "w"), separators=(",", ":"), sort_keys=True)
