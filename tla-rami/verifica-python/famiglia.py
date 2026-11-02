"""Traduzione a mano di Famiglia.tla (tla-rami) in Python: ricerca in ampiezza con la stessa VIEW
(impronte a 64 bit come TLC). Non e' TLC: serve a far girare i modelli quando TLC non si puo' eseguire.
Uso: python3 famiglia.py Chiave=valore ...   (le stesse costanti dei .cfg; DBs=2 o 3, Collectors=1)
     inv=Nome1,Nome2 per scegliere le invarianti."""
import sys, time
from collections import deque
C=dict(DBs=2,Readers=1,Collectors=1,MaxBoxes=3,MaxCycles=3,MaxPoints=1,Hold=0,RHold=1,Grace=1,
       Reread=0,NoRegisterFirst=0,SplitPin=0,OwnerDeletes=0,DropDeletesPrefix=0,NoRegCheck=0,
       AgeByCreation=0,CountSweeps=0,NoSlowCheck=0,Track=0,PinCap=0,FHold=0)
INV=['TypeOK','NoDangling','PinsLive','ReadersOK','InFlightLive']
NOTRACE='notrace' in sys.argv
for a in sys.argv[1:]:
    if a=='notrace': continue
    k,v=a.split('=')
    if k=='inv': INV=v.split(',')
    else: C[k]=int(v)
assert C['Collectors']==1, "simmetria tra raccoglitori non tradotta"
ND=C['DBs']; NB=C['MaxBoxes']; NR=C['Readers']; MC=C['MaxCycles']
DBS=range(ND); BOX=range(1,NB+1); ROOT=0
Hold,RHold,Grace,FHold=C['Hold'],C['RHold'],C['Grace'],C['FHold']
F=lambda k: bool(C[k])
E=frozenset()
# indici dei campi
(BS,OW,BC,NX,MF,MEM,PINS,USED,NP,FST,FPIN,FCYC,WST,WBOX,WIN,WCYC,
 CST,CCAND,CMEM,CPINS,CREAD,CUSED,CCYC,MARK,CYC,RST,RDB,RHELD,RCYC,RFAIL,SEEN)=range(31)
def tset(t,i,v): l=list(t); l[i]=v; return tuple(l)
def init():
    s=[None]*31
    s[BS]=tuple('l' if b==1 else 'n' for b in BOX)   # indice b-1
    s[OW]=tuple(ROOT if b==1 else None for b in BOX)
    s[BC]=tuple(0 for b in BOX); s[NX]=2
    s[MF]=tuple(frozenset({1}) if d==ROOT else E for d in DBS)
    s[MEM]=frozenset({ROOT}); s[PINS]=E; s[USED]=frozenset({ROOT}); s[NP]=0
    s[FST]=tuple('none' for d in DBS); s[FPIN]=tuple(E for d in DBS); s[FCYC]=tuple(0 for d in DBS)
    s[WST]=tuple('idle' for d in DBS); s[WBOX]=tuple(0 for d in DBS); s[WIN]=tuple(E for d in DBS); s[WCYC]=tuple(0 for d in DBS)
    s[CST]='idle'; s[CCAND]=E; s[CMEM]=E; s[CPINS]=E; s[CREAD]=E; s[CUSED]=E; s[CCYC]=0; s[MARK]=tuple(0 for b in BOX)
    s[CYC]=0
    s[RST]=tuple('idle' for r in range(NR)); s[RDB]=tuple(ROOT for r in range(NR)); s[RHELD]=tuple(E for r in range(NR)); s[RCYC]=tuple(0 for r in range(NR))
    s[RFAIL]=False; s[SEEN]=E
    return tuple(s)
def live(s): return frozenset(b for b in BOX if s[BS][b-1]=='l')
def pinboxes(P): 
    r=set()
    for e in P: r|=e[3]
    return frozenset(r)
def touch(s,l,S):
    if F('AgeByCreation'): return
    cy=s[CYC]; l[BC]=tuple(cy if b in S else s[BC][b-1] for b in BOX)
def mark(s,l,ev):
    if F('Track'): l[SEEN]=s[SEEN]|{ev}
def nexts(s):
    out=[]
    cy=s[CYC]; mem=s[MEM]
    for d in DBS:
        # FlushWrite / CompactWrite
        if d in mem and s[WST][d]=='idle' and s[NX]<=NB:
            nb=s[NX]
            def newbox(l):
                l[BS]=tset(s[BS],nb-1,'l'); l[OW]=tset(s[OW],nb-1,d); l[BC]=tset(s[BC],nb-1,cy)
                l[NX]=nb+1; l[WBOX]=tset(s[WBOX],d,nb); l[WCYC]=tset(s[WCYC],d,cy)
            l=list(s); newbox(l); l[WST]=tset(s[WST],d,'flush'); l[WIN]=tset(s[WIN],d,E); out.append(tuple(l))
            m=sorted(s[MF][d])
            subs=[frozenset({x}) for x in m]+[frozenset({x,y}) for i,x in enumerate(m) for y in m[i+1:]]
            for S in subs:
                l=list(s); newbox(l); l[WST]=tset(s[WST],d,'compact'); l[WIN]=tset(s[WIN],d,S); out.append(tuple(l))
        # WriteCommit
        if d in mem and s[WST][d]!='idle' and cy-s[WCYC][d]<=Hold:
            l=list(s)
            nmf=tset(s[MF],d,(s[MF][d]-s[WIN][d])|{s[WBOX][d]}); l[MF]=nmf; l[WST]=tset(s[WST],d,'idle')
            if F('OwnerDeletes'):
                inuse=set(pinboxes(s[PINS]))
                for e in DBS: inuse|=nmf[e]
                dl={b for b in s[WIN][d] if s[OW][b-1]==d and b not in inuse}
                l[BS]=tuple('g' if b in dl else s[BS][b-1] for b in BOX)
            touch(s,l,s[WIN][d]); out.append(tuple(l))
        # WriteAbort
        if s[WST][d]!='idle':
            l=list(s); l[WST]=tset(s[WST],d,'idle'); out.append(tuple(l))
        # ForkStart
        if d not in s[USED]:
            mfs={s[MF][x] for x in mem}
            srcs=mfs|{e[3] for e in s[PINS] if e[0]=='point'}
            for P in srcs:
                l=list(s)
                if P not in mfs: mark(s,l,'forkFromPoint')
                l[FPIN]=tset(s[FPIN],d,P); l[USED]=s[USED]|{d}
                l[FCYC]=tset(s[FCYC],d,cy)
                if F('SplitPin') or F('NoRegisterFirst'):
                    l[FST]=tset(s[FST],d,'read' if F('SplitPin') else 'pinned')
                else:
                    l[FST]=tset(s[FST],d,'pinned'); l[PINS]=s[PINS]|{('fork',d,0,P)}
                out.append(tuple(l))
        # ForkRegister
        if s[FST][d]=='read' and (not F('PinCap') or cy-s[FCYC][d]<=FHold):
            l=list(s); l[PINS]=s[PINS]|{('fork',d,0,s[FPIN][d])}; l[FST]=tset(s[FST],d,'pinned'); out.append(tuple(l))
        # ForkWrite
        if s[FST][d]=='pinned':
            l=list(s); l[MF]=tset(s[MF],d,s[FPIN][d]); l[FST]=tset(s[FST],d,'written'); out.append(tuple(l))
        # ForkJoin
        if s[FST][d]=='written':
            l=list(s); l[MEM]=mem|{d}; l[PINS]=frozenset(e for e in s[PINS] if not(e[0]=='fork' and e[1]==d))
            l[FST]=tset(s[FST],d,'done'); out.append(tuple(l))
        # ForkAbort
        if s[FST][d] in ('read','pinned','written'):
            l=list(s); l[MF]=tset(s[MF],d,E); l[PINS]=frozenset(e for e in s[PINS] if not(e[0]=='fork' and e[1]==d))
            l[FST]=tset(s[FST],d,'aborted'); touch(s,l,s[MF][d]|s[FPIN][d]); out.append(tuple(l))
        # PointCreate
        if d in mem and s[NP]<C['MaxPoints']:
            l=list(s); l[PINS]=s[PINS]|{('point',d,s[NP]+1,s[MF][d])}; l[NP]=s[NP]+1; out.append(tuple(l))
        # Drop
        if d in mem:
            l=list(s); l[MEM]=mem-{d}
            pts=frozenset(e for e in s[PINS] if e[0]=='point' and e[1]==d)
            l[PINS]=s[PINS]-pts; l[MF]=tset(s[MF],d,E); l[WST]=tset(s[WST],d,'idle')
            if F('DropDeletesPrefix'):
                l[BS]=tuple('g' if (s[OW][b-1]==d and s[BS][b-1]=='l') else s[BS][b-1] for b in BOX)
            touch(s,l,s[MF][d]|pinboxes(pts)); out.append(tuple(l))
    # PointExpire
    for e in s[PINS]:
        if e[0]=='point':
            l=list(s); l[PINS]=s[PINS]-{e}; touch(s,l,e[3]); out.append(tuple(l))
    # raccoglitore (uno solo)
    cst=s[CST]
    if cst=='idle' and cy<MC:
        l=list(s); l[CST]='listed'; l[CCAND]=live(s); l[CCYC]=cy; out.append(tuple(l))
    if cst=='listed':
        l=list(s); l[CST]='reg'; l[CMEM]=mem; l[CPINS]=s[PINS]; l[CREAD]=E; l[CUSED]=E; out.append(tuple(l))
    if cst=='reg':
        for d in s[CMEM]-s[CREAD]:
            l=list(s); l[CREAD]=s[CREAD]|{d}; l[CUSED]=s[CUSED]|s[MF][d]; out.append(tuple(l))
        if s[CREAD]==s[CMEM]:
            regch = mem!=s[CMEM] or s[PINS]!=s[CPINS]
            if (not F('NoRegCheck') and regch) or (not F('NoSlowCheck') and cy!=s[CCYC]):
                l=list(s); l[CST]='idle'; out.append(tuple(l))
            if cy<MC and (F('NoRegCheck') or not regch) and (F('NoSlowCheck') or cy==s[CCYC]):
                unused=s[CCAND]-(s[CUSED]|pinboxes(s[CPINS]))
                if F('CountSweeps'): dl={b for b in unused if s[MARK][b-1]+1>=Grace}
                else: dl={b for b in unused if cy-s[BC][b-1]>=Grace}
                l=list(s); l[BS]=tuple('g' if b in dl else s[BS][b-1] for b in BOX)
                if F('CountSweeps'): l[MARK]=tuple(s[MARK][b-1]+1 if b in unused-dl else 0 for b in BOX)
                if F('Track'):
                    ev=set()
                    if dl: ev.add('deleted')
                    if any(s[OW][b-1] is not None and s[OW][b-1] in s[USED]-mem and s[FST][s[OW][b-1]]!='aborted' for b in dl): ev.add('reclaimDropped')
                    l[SEEN]=s[SEEN]|ev
                l[CST]='idle'; l[CYC]=cy+1; out.append(tuple(l))
    # lettori
    for r in range(NR):
        if s[RST][r]=='idle':
            for d in mem:
                l=list(s); l[RST]=tset(s[RST],r,'hold'); l[RDB]=tset(s[RDB],r,d); l[RHELD]=tset(s[RHELD],r,s[MF][d]); l[RCYC]=tset(s[RCYC],r,cy); out.append(tuple(l))
        if s[RST][r]=='hold':
            if F('Reread') or cy-s[RCYC][r]<=RHold:
                l=list(s); rd=s[RDB][r]
                if s[RHELD][r]<=live(s): l[RST]=tset(s[RST],r,'idle')
                elif F('Reread'):
                    if rd in mem:
                        l[RHELD]=tset(s[RHELD],r,s[MF][rd]); l[RCYC]=tset(s[RCYC],r,cy); mark(s,l,'reread')
                    else: l[RST]=tset(s[RST],r,'idle')
                else:
                    if rd in mem: l[RFAIL]=True
                    else: l[RST]=tset(s[RST],r,'idle')
                out.append(tuple(l))
            l=list(s); l[RST]=tset(s[RST],r,'idle'); out.append(tuple(l))
    return out
def view(s):
    return (s[BS],s[OW],s[BC],s[NX],s[MF],s[MEM],s[PINS],s[USED],s[NP],s[FST],
            tuple(s[FPIN][d] if s[FST][d] in ('read','pinned') else E for d in DBS),
            tuple(s[FCYC][d] if (F('PinCap') and s[FST][d]=='read') else 0 for d in DBS),
            tuple(('idle',) if s[WST][d]=='idle' else (s[WST][d],s[WBOX][d],s[WIN][d],s[WCYC][d]) for d in DBS),
            ('idle',) if s[CST]=='idle' else (s[CST],s[CCAND],s[CMEM],s[CPINS],s[CREAD],s[CUSED],s[CCYC]),
            s[MARK] if F('CountSweeps') else 0, s[CYC],
            tuple(('idle',) if s[RST][r]=='idle' else (s[RDB][r],s[RHELD][r],s[RCYC][r]) for r in range(NR)),
            s[RFAIL],s[SEEN])
def check(s):
    L=live(s)
    for k in INV:
        if k=='TypeOK': ok=0<=s[CYC]<=MC and all(0<=x<=MC for x in s[BC]) and all(0<=x<=MC for x in s[MARK])
        elif k=='NoDangling': ok=all(s[MF][d]<=L for d in DBS if d in s[MEM] or s[FST][d]=='written')
        elif k=='PinsLive': ok=pinboxes(s[PINS])<=L
        elif k=='ReadersOK': ok=not s[RFAIL]
        elif k=='InFlightLive': ok=all(s[WBOX][d] in L for d in s[MEM] if s[WST][d]!='idle' and s[CYC]-s[WCYC][d]<=Hold)
        elif k.startswith('SanityNo'): ok={'SanityNoDelete':'deleted','SanityNoReclaimDropped':'reclaimDropped','SanityNoReread':'reread','SanityNoForkFromPoint':'forkFromPoint'}[k] not in s[SEEN]
        else: raise SystemExit('invariante non tradotta: '+k)
        if not ok: return k
    return None
def show(s):
    names=['boxSt','owner','bcyc','nextBox','mf','members','pins','used','nPoints','fst','fpin','fcyc','wst','wbox','win','wcyc','cst','ccand','cmem','cpins','cread','cused','ccyc','marked','cycle','rst','rdb','rheld','rcyc','rfail','seen']
    f=lambda v: sorted(v) if isinstance(v,frozenset) else ([sorted(x) if isinstance(x,frozenset) else x for x in v] if isinstance(v,tuple) else v)
    return ' '.join(f'{n}={f(v)}' for n,v in zip(names,s) if n not in ('marked','seen'))
t0=time.time()
s0=init(); seen={hash(view(s0)):None}; parent={}
q=deque([s0]); gen=1; bad=check(s0); last=s0; depth={hash(view(s0)):0}
TRACE=len(sys.argv)>0
while q and not bad:
    s=q.popleft()
    for t in nexts(s):
        gen+=1; h=hash(view(t))
        if h in seen: continue
        seen[h]=None if NOTRACE else s
        b=check(t)
        if b: bad=b; last=t; break
        q.append(t)
print(f"costanti={ {k:v for k,v in C.items()} } invarianti={INV}")
print(f"generati={gen} distinti={len(seen)} in coda={len(q)} tempo={time.time()-t0:.0f}s")
if bad and NOTRACE: print(f'VIOLATA {bad} (traccia non tenuta: rifare senza notrace)')
elif bad:
    tr=[last]
    while True:
        p=seen[hash(view(tr[-1]))]
        if p is None: break
        tr.append(p)
    print(f"VIOLATA {bad}, traccia di {len(tr)} stati:")
    for i,x in enumerate(reversed(tr),1): print(f" {i}: {show(x)}")
else: print("nessuna violazione, modello completo")
