# a byte-for-byte TCP relay (no HTTP parsing): port $1 -> 127.0.0.1:$2, optional delay per chunk in ms ($3)
import asyncio, sys
LP, RP, D = int(sys.argv[1]), int(sys.argv[2]), float(sys.argv[3]) / 1000 if len(sys.argv) > 3 else 0
async def pipe(r, w):
    try:
        while (b := await r.read(65536)):
            if D: await asyncio.sleep(D)
            w.write(b); await w.drain()
    except Exception: pass
    finally: w.close()
async def handle(cr, cw):
    sr, sw = await asyncio.open_connection("127.0.0.1", RP)
    await asyncio.gather(pipe(cr, sw), pipe(sr, cw))
async def main():
    s = await asyncio.start_server(handle, "127.0.0.1", LP)
    async with s: await s.serve_forever()
asyncio.run(main())
