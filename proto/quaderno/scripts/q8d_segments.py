import sys,os,zlib,struct,glob
for d in sys.argv[1:]:
    print('==',d)
    for f in sorted(glob.glob(os.path.join(d,'seg-*.log'))):
        b=open(f,'rb').read(); p=0; first=last=None; n=0
        while p+8<=len(b):
            ln,crc=struct.unpack_from('<II',b,p)
            if ln==0 or p+8+ln>len(b) or zlib.crc32(b[p+8:p+8+ln])!=crc: break
            idx=struct.unpack_from('<Q',b,p+16)[0]
            if last is not None and idx!=last+1: break
            first = idx if first is None else first; last=idx; n+=1; p+=8+ln
        print(os.path.basename(f), 'entries', first, '..', last, 'n', n)
