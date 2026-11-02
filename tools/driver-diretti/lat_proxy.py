"""Proxy TCP che aggiunge un ritardo fisso in ogni verso: simula un cameriere in un'altra zona.
Uso: python3 lat_proxy.py LISTEN_IP:PORT TARGET_IP:PORT RITARDO_MS (ritardo per verso; il giro completo ne aggiunge il doppio)."""
import asyncio
import sys


async def pipe(r, w, delay):
    try:
        while True:
            data = await r.read(65536)
            if not data:
                break
            await asyncio.sleep(delay)
            w.write(data)
            await w.drain()
    except Exception:
        pass
    finally:
        try:
            w.close()
        except Exception:
            pass


async def main():
    lh, lp = sys.argv[1].rsplit(":", 1)
    th, tp = sys.argv[2].rsplit(":", 1)
    delay = float(sys.argv[3]) / 1000

    async def handle(cr, cw):
        try:
            sr, sw = await asyncio.open_connection(th, int(tp))
        except Exception:
            cw.close()
            return
        await asyncio.gather(pipe(cr, sw, delay), pipe(sr, cw, delay))

    srv = await asyncio.start_server(handle, lh, int(lp))
    print(f"proxy {sys.argv[1]} -> {sys.argv[2]} +{sys.argv[3]} ms per verso", flush=True)
    async with srv:
        await srv.serve_forever()


asyncio.run(main())
