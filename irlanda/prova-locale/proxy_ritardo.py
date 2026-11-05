"""Prova locale P1 Irlanda v2 · un proxy TCP che aggiunge ritardo, per simulare la rete tra il cliente e Atlas.
  RITARDO_MS per verso (un viaggio di andata e ritorno = 2 x RITARDO_MS) e, all'apertura di ogni connessione,
  APERTURA_MS in piu' (al posto di TLS e autenticazione di Atlas, che il nostro server locale non ha).
Mac via VPN: hello p50 34 ms (MISURATO in P1) -> RITARDO_MS 17, APERTURA_MS circa 3 viaggi = 100 (STIMATO).
Macchina in eu-west-1: viaggio circa 1 ms (STIMATO) -> RITARDO_MS 0.5, APERTURA_MS 3.
Uso: proxy_ritardo.py PORTA_ASCOLTO HOST:PORTA_DESTINAZIONE RITARDO_MS APERTURA_MS"""
import asyncio
import sys

ascolto, dest, rit, apertura = int(sys.argv[1]), sys.argv[2], float(sys.argv[3]) / 1000, float(sys.argv[4]) / 1000
dh, dp = dest.rsplit(":", 1)


async def tubo(r, w):
    loop = asyncio.get_running_loop()
    coda = asyncio.Queue()

    async def leggi():
        while True:
            b = await r.read(65536)
            await coda.put((loop.time() + rit, b))
            if not b:
                return

    async def scrivi():
        while True:
            t, b = await coda.get()
            d = t - loop.time()
            if d > 0:
                await asyncio.sleep(d)
            if not b:
                try:
                    w.write_eof()
                except Exception:
                    pass
                return
            w.write(b)
            await w.drain()
    try:
        await asyncio.gather(leggi(), scrivi())
    except Exception:
        pass
    finally:
        try:
            w.close()
        except Exception:
            pass


async def gestisci(cr, cw):
    await asyncio.sleep(apertura)
    try:
        sr, sw = await asyncio.open_connection(dh, int(dp))
    except Exception:
        cw.close()
        return
    await asyncio.gather(tubo(cr, sw), tubo(sr, cw))


async def main():
    srv = await asyncio.start_server(gestisci, "127.0.0.1", ascolto)
    async with srv:
        await srv.serve_forever()

asyncio.run(main())
