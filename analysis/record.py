"""Raw capture of both Lighter venues for every perp listed on both: every websocket message (order book snapshot and
each delta, every trade, market stats), stamped with the local receive time, as newline-delimited JSON, gzipped,
one file per venue per UTC hour: data/raw_{venue}_{YYYYMMDD_HH}.jsonl.gz. Each line: {"t": unix seconds with
microseconds, "v": venue, "m": the message as received}. Keepalive is the venue's application-level ping."""

from __future__ import annotations

import asyncio
import gzip
import json
import time
from datetime import UTC, datetime
from pathlib import Path

import websockets
from lighter.api.order_api import OrderApi
from lighter.api_client import ApiClient
from lighter.configuration import Configuration

VENUES = {"lighter": "https://mainnet.zklighter.elliot.ai", "rh": "https://api.rh.lighter.xyz"}
OUT = Path(__file__).parent / "data"


async def capture(venue: str, url: str, market_ids: list[int]) -> None:
    hour, fh, delay = None, None, 1.0
    while True:
        started = time.time()
        try:
            async with websockets.connect(url.replace("https://", "wss://") + "/stream", ping_interval=None, max_size=None) as ws:
                for mid in market_ids:
                    await ws.send(json.dumps({"type": "subscribe", "channel": f"order_book/{mid}"}))
                    await ws.send(json.dumps({"type": "subscribe", "channel": f"trade/{mid}"}))
                await ws.send('{"type":"subscribe","channel":"market_stats/all"}')
                last = pinged = time.time()
                print(f"{venue}: connected, {len(market_ids)} markets", flush=True)
                while True:
                    try:
                        raw = await asyncio.wait_for(ws.recv(), 20)
                    except TimeoutError:
                        if time.time() - last > 60:
                            raise ConnectionError("silent for 60 s") from None
                        raw = ""
                    now = time.time()
                    if now - pinged > 20:  # the venue's keepalive is the client's ping, busy stream or not: without it, it closes every two minutes
                        await ws.send('{"type":"ping"}')
                        pinged = now
                    if not raw:
                        continue
                    last = now
                    this_hour = datetime.fromtimestamp(now, UTC).strftime("%Y%m%d_%H")
                    if this_hour != hour:
                        if fh:
                            fh.close()
                        hour, fh = this_hour, gzip.open(OUT / f"raw_{venue}_{this_hour}.jsonl.gz", "at")
                    fh.write(f'{{"t":{now:.6f},"v":"{venue}","m":{raw if isinstance(raw, str) else raw.decode()}}}\n')
        except Exception as e:
            print(f"{venue}: reconnect after {e!r}"[:200], flush=True)
        if fh:
            fh.flush()
        delay = 1.0 if time.time() - started > 60 else min(delay * 2, 60.0)
        await asyncio.sleep(delay)


async def main() -> None:
    OUT.mkdir(exist_ok=True)
    markets: dict[str, dict[str, int]] = {}
    for name, url in VENUES.items():
        api = ApiClient(configuration=Configuration(host=url))
        markets[name] = {ob.symbol: ob.market_id for ob in (await OrderApi(api).order_books()).order_books if ob.market_type == "perp"}
        await api.close()
    shared = sorted(set(markets["lighter"]) & set(markets["rh"]))
    (OUT / "markets.json").write_text(json.dumps({v: {s: markets[v][s] for s in shared} for v in VENUES}, indent=1))
    print(f"recording {len(shared)} shared markets: {' '.join(shared)}", flush=True)
    await asyncio.gather(*(capture(name, url, [markets[name][s] for s in shared]) for name, url in VENUES.items()))


if __name__ == "__main__":
    asyncio.run(main())
