# genera i file Famiglia-*.cfg
base=dict(DBs="{d1, d2}",Readers="{r1}",MaxBoxes=3,MaxCycles=3,MaxPoints=1,Hold=0,RHold=0,Grace=2,Reread="FALSE",
 NoRegisterFirst="FALSE",SplitPin="FALSE",OwnerDeletes="FALSE",DropDeletesPrefix="FALSE",
 NoRegCheck="FALSE",AgeByCreation="FALSE",Track="FALSE",Collectors="{c1}")
inv="TypeOK NoDangling PinsLive ReadersOK InFlightLive"
cfgs={
 "base":{},
 "reread":dict(Reread="TRUE"),
 "due-raccoglitori":dict(Collectors="{c1, c2}"),
 "catena":dict(DBs="{d1, d2, d3}",Readers="{}"),
 "grande":dict(Grace=3,Hold=1,RHold=1,MaxCycles=4),
 # negativi
 "noregisterfirst":dict(NoRegisterFirst="TRUE"),
 "splitpin":dict(SplitPin="TRUE"),
 "ownerdeletes":dict(OwnerDeletes="TRUE"),
 "dropdeletesprefix":dict(DropDeletesPrefix="TRUE"),
 "noregcheck":dict(NoRegCheck="TRUE"),
 "nograce":dict(Grace=1),
 "nograce-reread":dict(Grace=1,Reread="TRUE"),
 "hold-troppo":dict(Hold=1),
 "lettore-vecchio":dict(RHold=1),
 "agebycreation":dict(Grace=3,Hold=1,RHold=1,MaxCycles=4,AgeByCreation="TRUE"),
 # controlli che il modello non sia vuoto (devono essere violati)
 "sanity-delete":dict(Track="TRUE",_inv="SanityNoDelete"),
 "sanity-orfano":dict(Track="TRUE",_inv="SanityNoOrphanParent"),
 "sanity-raccolta":dict(Track="TRUE",_inv="SanityNoReclaimDropped"),
 "sanity-rilettura":dict(Track="TRUE",Reread="TRUE",_inv="SanityNoReread"),
 "sanity-punto":dict(Track="TRUE",_inv="SanityNoForkFromPoint"),
 "sanity-catena":dict(DBs="{d1, d2, d3}",Readers="{}",Track="TRUE",_inv="SanityNoGrandchild"),
}
for n,o in cfgs.items():
  c=dict(base); c.update(o); i=c.pop("_inv",inv)
  sym = "SYMMETRY Sym\n" if c["Collectors"]=="{c1, c2}" else ""
  lines=["SPECIFICATION Spec","CONSTANTS","  Root = d1",
   "  d1 = d1","  d2 = d2","  d3 = d3","  c1 = c1","  c2 = c2","  r1 = r1"]
  lines+= [f"  {k} = {v}" for k,v in c.items()]
  open(f"Famiglia-{n}.cfg","w").write("\n".join(lines)+"\n"+sym+"VIEW View\n"+f"INVARIANTS {i}\n")
