"""Traduzione a mano di Testimone.tla (tla-testimone) in Python, BFS con simmetria su {a,b}.
Non e' TLC: serve solo a controllare i controlli negativi quando TLC non si puo' eseguire.
Uso: python3 testimone.py MaxEpoch MaxLen MaxLoss MaxRestart [Flag=1 ...] [inv=Nome,...]"""
import sys
from collections import deque
A,B=0,1
args=sys.argv[1:]
ME,ML,MLOSS,MR=map(int,args[:4])
F={k:False for k in "AckOnOne NoWitnessGrant NoReceipt NoReceiptCheck WitnessDoubleVote WitnessAmnesia NoOwnEpochRule NoVoteLogCheck ReplicateToHigher NoAckReset".split()}
INV="TypeOK ElectionSafety NoSplitBrain LeaderCompleteness DurabilityPieno".split()
for a in args[4:]:
    k,v=a.split('=')
    if k=='inv': INV=v.split(',')
    else: F[k]=v=='1'
# state: (epoch, role, mode, log, alive, match, wEpoch, wRec, cmt, cmtAt, pienoMax, split, degBy, losses, restarts)
# per-copy tuples of len 2; role 1=capo; mode 1=degradato; degBy frozenset->tuple of bools
def upd(t,i,v): l=list(t); l[i]=v; return tuple(l)
def lastep(s): return s[-1] if s else 0
def atleast(s,ep,n): return lastep(s)>ep or (lastep(s)==ep and len(s)>=n)
def isprefix(p,s): return len(p)<=len(s) and s[:len(p)]==p
def confirm(p,e,cmt,cmtAt,split):
    if isprefix(p,cmt): return cmt,cmtAt,split
    if isprefix(cmt,p): return p, cmtAt+tuple([e]*(len(p)-len(cmt))), split
    return cmt,cmtAt,True
def swap(s):
    ep,ro,mo,lg,al,ma,we,wr,cmt,cmtAt,pm,sp,deg,lo,re=s
    sw=lambda t:(t[1],t[0])
    return (sw(ep),sw(ro),sw(mo),sw(lg),sw(al),sw(ma),we,wr,cmt,cmtAt,pm,sp,sw(deg),lo,re)
def canon(s): return min(s,swap(s))
init=((0,0),(0,0),(0,0),((),()),(True,True),(0,0),0,(0,0),(),(),0,False,(False,False),0,0)
def nexts(s):
    ep,ro,mo,lg,al,ma,we,wr,cmt,cmtAt,pm,sp,deg,lo,re=s
    out=[]
    for c in (A,B):
        v=1-c
        for e in range(1,ME+1):
            # ElectByCopy
            if al[c] and al[v] and ep[c]<e and ep[v]<e and (F['NoVoteLogCheck'] or atleast(lg[c],lastep(lg[v]),len(lg[v]))):
                nro=upd(upd(ro,c,1),v,0); nmo=(0,0)
                nma=ma if F['NoAckReset'] else upd(ma,c,0)
                out.append((upd(upd(ep,c,e),v,e),nro,nmo,lg,al,nma,we,wr,cmt,cmtAt,pm,sp,deg,lo,re))
            # ElectByWitness
            if al[c] and ep[c]<e and (we<=e if F['WitnessDoubleVote'] else we<e) and (F['NoReceiptCheck'] or atleast(lg[c],wr[0],wr[1])):
                nma=ma if F['NoAckReset'] else upd(ma,c,0)
                out.append((upd(ep,c,e),upd(ro,c,1),upd(mo,c,0),lg,al,nma,e,wr,cmt,cmtAt,pm,sp,deg,lo,re))
        l=c; f=v
        # ClientWrite
        if al[l] and ro[l]==1 and len(lg[l])<ML:
            out.append((ep,ro,mo,upd(lg,l,lg[l]+(ep[l],)),al,ma,we,wr,cmt,cmtAt,pm,sp,deg,lo,re))
        # Replicate
        if al[l] and al[f] and ro[l]==1 and (F['ReplicateToHigher'] or ep[f]<=ep[l]) and (lg[f]!=lg[l] or ma[l]!=len(lg[l])):
            out.append((upd(ep,f,ep[l]),upd(ro,f,0),upd(mo,f,0),upd(lg,f,lg[l]),al,upd(ma,l,len(lg[l])),we,wr,cmt,cmtAt,pm,sp,deg,lo,re))
        # EnterDegraded
        if al[l] and ro[l]==1 and mo[l]==0:
            if F['NoWitnessGrant']:
                out.append((ep,ro,upd(mo,l,1),lg,al,ma,we,wr,cmt,cmtAt,pm,sp,deg,lo,re))
            elif we<=ep[l]:
                out.append((ep,ro,upd(mo,l,1),lg,al,ma,ep[l],wr,cmt,cmtAt,pm,sp,deg,lo,re))
        # LeaveDegraded
        if al[l] and ro[l]==1 and mo[l]==1:
            out.append((ep,ro,upd(mo,l,0),lg,al,ma,we,wr,cmt,cmtAt,pm,sp,deg,lo,re))
        # Restart
        if al[c] and ro[c]==1:
            out.append((ep,upd(ro,c,0),upd(mo,c,0),lg,al,ma,we,wr,cmt,cmtAt,pm,sp,deg,lo,re))
        # Lose
        if al[c] and lo<MLOSS:
            out.append((ep,ro,mo,lg,upd(al,c,False),ma,we,wr,cmt,cmtAt,pm,sp,deg,lo+1,re))
        for n in range(1,ML+1):
            # CommitPieno
            if al[l] and ro[l]==1 and 1<=n<=len(lg[l]) and (F['AckOnOne'] or n<=ma[l]):
                c2,ca2,sp2=confirm(lg[l][:n],ep[l],cmt,cmtAt,sp)
                out.append((ep,ro,mo,lg,al,ma,we,wr,c2,ca2,max(n,pm),sp2,deg,lo,re))
            # CommitDegraded
            if al[l] and ro[l]==1 and mo[l]==1 and 1<=n<=len(lg[l]) and (F['NoOwnEpochRule'] or lg[l][n-1]==ep[l]):
                if F['NoWitnessGrant'] or F['NoReceipt']:
                    if not (F['NoWitnessGrant'] or we==ep[l]): continue
                    nwr=wr
                else:
                    if we!=ep[l]: continue
                    nwr=(lg[l][n-1],n) if atleast(lg[l][:n],wr[0],wr[1]) else wr
                c2,ca2,sp2=confirm(lg[l][:n],ep[l],cmt,cmtAt,sp)
                out.append((ep,ro,mo,lg,al,ma,we,nwr,c2,ca2,pm,sp2,upd(deg,l,True),lo,re))
    # WitnessReset
    if F['WitnessAmnesia'] and re<MR:
        out.append((ep,ro,mo,lg,al,ma,0,(0,0),cmt,cmtAt,pm,sp,deg,lo,re+1))
    return out
def check(s):
    ep,ro,mo,lg,al,ma,we,wr,cmt,cmtAt,pm,sp,deg,lo,re=s
    r={}
    r['TypeOK']=all(0<=x<=ME for x in ep) and all(0<=x<=ML for x in ma) and 0<=we<=ME and 0<=wr[0]<=ME and 0<=wr[1]<=ML and 0<=pm<=ML
    r['ElectionSafety']=not(ro[0]==1 and ro[1]==1 and ep[0]==ep[1])
    r['NoSplitBrain']=not sp
    r['LeaderCompleteness']=all(not(al[l] and ro[l]==1) or all(cmtAt[i]>ep[l] or (i<len(lg[l]) and lg[l][i]==cmt[i]) for i in range(len(cmt))) for l in (A,B))
    r['DurabilityPieno']=all(not al[c] or (pm<=len(lg[c]) and lg[c][:pm]==cmt[:pm]) for c in (A,B))
    r['SanityNoDegradedCommit']=not any(deg)
    r['SanityNoFailover']=not any(al[c] and ro[c]==1 and not al[1-c] and deg[1-c] and not deg[c] and len(cmt)>=1 and cmtAt[-1]<ep[c] for c in (A,B))
    for k in INV:
        if not r[k]: return k
    return None
def run():
    s0=canon(init); seen={s0:None}; q=deque([s0]); gen=1
    bad=check(s0)
    if bad: return bad,s0,seen,gen
    while q:
        s=q.popleft()
        for t in nexts(s):
            gen+=1; t=canon(t)
            if t in seen: continue
            seen[t]=s
            bad=check(t)
            if bad: return bad,t,seen,gen
            q.append(t)
    return None,None,seen,gen
bad,t,seen,gen=run()
print(f"flags={[k for k,v in F.items() if v]} generati={gen} distinti={len(seen)}")
if bad:
    tr=[]
    while t is not None: tr.append(t); t=seen[t]
    print(f"VIOLATA {bad}, traccia di {len(tr)} stati:")
    for i,x in enumerate(reversed(tr),1):
        ep,ro,mo,lg,al,ma,we,wr,cmt,cmtAt,pm,sp,deg,lo,re=x
        print(f" {i}: epoch={ep} capo={ro} degr={mo} log={lg} vivo={al} match={ma} wEpoch={we} wRec={wr} cmt={cmt} cmtAt={cmtAt} pienoMax={pm} split={sp}")
else:
    print("nessuna violazione, modello completo")
