import time,sys,json
out=open(sys.argv[1],'a')
def rd():
    d={}
    for l in open('/proc/stat'):
        if l.startswith('cpu') and l[3]!=' ':
            f=[int(x) for x in l.split()[1:]]; d[l.split()[0]]=(f[3]+f[4],sum(f))
    return d
a=rd()
while True:
    time.sleep(1); b=rd(); t=time.time()
    out.write(json.dumps({'t':t,**{k:round(100*(1-(b[k][0]-a[k][0])/max(1,b[k][1]-a[k][1])),1) for k in b}})+'\n'); out.flush(); a=b
