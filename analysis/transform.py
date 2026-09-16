"""Raw websocket capture -> analysable tables.

    transform.py hour data/raw_lighter_20260916_03.jsonl.gz   one raw file -> derived/parts/<venue>_<hour>_<kind>.parquet
    transform.py merge                                         parts -> derived/*.parquet and *.csv (both venues joined)

Run from this folder, after record.py has filled data/.

Per venue: bbo (every top-of-book change), trades (every print), stats (index, mark, funding, volume as the venue reports
them), grid (one row per market per second: top of book and the price reached buying or selling 100 / 500 / 2000 USD).
Joined: pair_1s (both venues per market per second with the gap and the executable cross), cross_episodes_touch (runs
of the touches being crossed, event resolution), cross_episodes_sized (runs of an executable cross at 100 / 500 / 2000
USD, second resolution), summary_by_market, coverage. Times are the recorder's receive time (unix seconds, one clock for
both venues); `sec` is the integer second a grid row describes the state at the start of.
"""

from __future__ import annotations

import calendar
import json
import math
import sys
import time
import zlib
from pathlib import Path

import orjson
import pandas as pd

ROOT = Path(__file__).resolve().parent
RAW, PARTS, OUT = ROOT / "data", ROOT / "derived" / "parts", ROOT / "derived"
CLIPS = (100, 500, 2000)
BRIDGE_S = 5  # a hole in a run (the recorder reconnecting) no longer than this does not end the run
NAN = float("nan")


def impact(levels: dict[float, float], usd: float, asks: bool) -> float:
    """Price reached walking `usd` of notional through one side of a book; NaN if the book does not hold that much."""
    need = usd
    for p in sorted(levels, reverse=not asks):
        need -= p * levels[p]
        if need <= 0:
            return p
    return NAN


def iter_lines(path: Path):
    """Lines of a gzip file that may hold several members, truncated where a process was killed: each member is read as far as it
    goes, then the next member header is searched for and reading resumes there. Only complete lines are yielded."""
    raw = path.read_bytes()
    pos, tail = 0, b""
    while pos < len(raw):
        d = zlib.decompressobj(wbits=31)
        try:
            out = d.decompress(raw[pos:])
            pos = len(raw) - len(d.unused_data)
            if not d.eof:
                pos = len(raw)
        except zlib.error:
            out = b""
            try:  # salvage what the broken member yields before the error, chunk by chunk
                d = zlib.decompressobj(wbits=31)
                for i in range(pos, len(raw), 1 << 20):
                    out += d.decompress(raw[i : i + (1 << 20)])
            except zlib.error:
                pass
            nxt = raw.find(b"\x1f\x8b\x08", pos + 1)
            pos = nxt if nxt > 0 else len(raw)
        lines = (tail + out).split(b"\n")
        tail = lines.pop()
        yield from lines


def feed(path: Path, sym: dict[str, str], books: dict, top: dict, out: dict | None) -> None:
    """Replay one raw file into `books` and `top`; with `out` given, also emit the bbo, trades, stats and grid rows."""
    seen: set[int] = set()
    sec = None
    for line in iter_lines(path):
        try:
            r = orjson.loads(line)
        except orjson.JSONDecodeError:
            continue
        t, m = r["t"], r["m"]
        ty = m.get("type", "")
        if out is not None:
            if sec is None:
                sec = math.floor(t)
            while t >= sec + 1:  # a second boundary passed: the state now is the state at the start of the next second
                sec += 1
                for mid, b in books.items():
                    if b["bids"] and b["asks"]:
                        out["grid"].append((sec, sym[mid], max(b["bids"]), min(b["asks"]), *(impact(b["asks"], c, True) for c in CLIPS), *(impact(b["bids"], c, False) for c in CLIPS)))
        if ty == "connected":  # a new connection: every book is unknown until its snapshot arrives
            books.clear()
            if out is not None:
                for mid in top:
                    out["bbo"].append((t, sym[mid], NAN, NAN, NAN, NAN))
            top.clear()
        elif ty.endswith("/order_book"):
            mid = m["channel"].split(":")[1]
            if mid not in sym:
                continue
            if ty == "subscribed/order_book":
                books[mid] = {"bids": {}, "asks": {}}
            b = books.get(mid)
            if b is None:
                continue
            for side in ("bids", "asks"):
                for o in m["order_book"].get(side) or ():
                    p, s = float(o["price"]), float(o["size"])
                    if s == 0:
                        b[side].pop(p, None)
                    else:
                        b[side][p] = s
            if b["bids"] and b["asks"]:
                bid, ask = max(b["bids"]), min(b["asks"])
                cur = (bid, ask, b["bids"][bid], b["asks"][ask])
                if top.get(mid) != cur:
                    top[mid] = cur
                    if out is not None:
                        out["bbo"].append((t, sym[mid], *cur))
        elif out is None:
            continue
        elif ty == "update/trade":
            mid = m["channel"].split(":")[1]
            if mid not in sym:
                continue
            for kind in ("trades", "liquidation_trades"):
                for tr in m.get(kind) or ():
                    tid = int(tr["trade_id"])
                    if tid in seen:
                        continue
                    seen.add(tid)
                    out["trades"].append((t, sym[mid], float(tr["price"]), float(tr["size"]), "sell" if tr["is_maker_ask"] is False else "buy", kind == "liquidation_trades", tid, int(tr["timestamp"]) / 1000))
        elif ty.endswith("/market_stats"):
            for mid, st in (m.get("market_stats") or {}).items():
                if mid in sym:
                    out["stats"].append((t, sym[mid], float(st["index_price"]), float(st["mark_price"]), float(st["last_trade_price"]), float(st["funding_rate"]), float(st["daily_quote_token_volume"])))


def hour(path: Path) -> None:
    venue, stamp = path.name.removeprefix("raw_").removesuffix(".jsonl.gz").split("_", 1)
    ids = json.loads((RAW / "markets.json").read_text())[venue]
    sym = {str(i): s for s, i in ids.items()}
    books: dict[str, dict[str, dict[float, float]]] = {}  # market id -> {"bids": {price: size}, "asks": {...}}
    top: dict[str, tuple] = {}
    files = sorted(RAW.glob(f"raw_{venue}_*.jsonl.gz"))
    i = files.index(path.resolve())
    if i > 0:  # the book state at the start of this hour is whatever the previous hour left: replay it first, emitting nothing
        feed(files[i - 1], sym, books, top, None)
    out: dict[str, list] = {"bbo": [], "trades": [], "stats": [], "grid": []}
    if top:  # carried tops open this hour's bbo stream, so an episode running across the boundary is not lost
        for mid, cur in top.items():
            out["bbo"].append((calendar.timegm(time.strptime(stamp, "%Y%m%d_%H")), sym[mid], *cur))
    feed(path, sym, books, top, out)
    PARTS.mkdir(parents=True, exist_ok=True)
    frames = {
        "bbo": pd.DataFrame(out["bbo"], columns=["t", "sym", "bid", "ask", "bid_sz", "ask_sz"]),
        "trades": pd.DataFrame(out["trades"], columns=["t", "sym", "price", "size", "taker", "liquidation", "trade_id", "venue_ts"]),
        "stats": pd.DataFrame(out["stats"], columns=["t", "sym", "index", "mark", "last", "funding", "volume_usd"]),
        "grid": pd.DataFrame(out["grid"], columns=["sec", "sym", "bid", "ask", *(f"buy{c}" for c in CLIPS), *(f"sell{c}" for c in CLIPS)]),
    }
    for kind, df in frames.items():
        df.to_parquet(PARTS / f"{venue}_{stamp}_{kind}.parquet", index=False)
    print(f"{path.name}: bbo {len(out['bbo'])} trades {len(out['trades'])} stats {len(out['stats'])} grid {len(out['grid'])}", flush=True)


def episodes_touch(bbo_l: pd.DataFrame, bbo_r: pd.DataFrame) -> pd.DataFrame:
    """Runs of one venue's bid above the other's ask, from the merged event streams of both venues' tops.
    Ends when the cross closes or either book becomes unknown."""
    rows = []
    for s in sorted(set(bbo_l.sym) & set(bbo_r.sym)):
        ev = pd.concat([bbo_l[bbo_l.sym == s].assign(v=0), bbo_r[bbo_r.sym == s].assign(v=1)]).sort_values("t", kind="stable")
        tops = [(NAN, NAN), (NAN, NAN)]
        start, hole = None, None  # hole: when a run was interrupted by a book going unknown; it resumes if the cross is back within BRIDGE_S
        sd, area, peak, t_prev, x_prev = 0, 0.0, 0.0, 0.0, 0.0
        for t, v, bid, ask in zip(ev.t.values, ev.v.values, ev.bid.values, ev.ask.values):
            tops[v] = (bid, ask)
            (lb, la), (rb, ra) = tops
            unknown = math.isnan(lb) or math.isnan(rb)
            if unknown:
                x, d = 0.0, 0
            else:
                x1, x2 = lb / ra - 1, rb / la - 1
                x, d = (x1, 1) if x1 >= x2 else (x2, -1)  # +1: Lighter bid over RH ask (sell Lighter, buy RH); -1 the other way
            if start is not None and unknown:
                if hole is None:
                    hole = t
                continue
            if start is not None and hole is not None:  # both books known again after a hole
                if x > 0 and d == sd and t - hole <= BRIDGE_S:
                    hole = None  # the cross carried on across the hole: keep the run, the hole counts as crossed at its last value
                else:
                    dur = hole - start
                    rows.append((s, start, hole, dur, sd, peak * 1e4, (area / dur if dur else peak) * 1e4))
                    start, hole = None, None
            if start is None:
                if x > 0:
                    start, sd, area, peak, t_prev, x_prev = t, d, 0.0, x, t, x
            else:
                area += x_prev * (t - t_prev)
                t_prev, x_prev = t, x
                peak = max(peak, x)
                if x <= 0 or d != sd:
                    dur = t - start
                    rows.append((s, start, t, dur, sd, peak * 1e4, (area / dur if dur else peak) * 1e4))
                    start = None
                    if x > 0:
                        start, sd, area, peak, t_prev, x_prev = t, d, 0.0, x, t, x
        if start is not None:  # still crossed when the capture ends
            end = hole if hole is not None else t_prev
            dur = end - start
            rows.append((s, start, end, dur, sd, peak * 1e4, (area / dur if dur else peak) * 1e4))
    return pd.DataFrame(rows, columns=["sym", "start", "end", "duration_s", "direction", "max_bps", "mean_bps"])


def episodes_sized(pair: pd.DataFrame) -> pd.DataFrame:
    """Runs of consecutive seconds with an executable cross at each clip."""
    rows = []
    for clip in CLIPS:
        col = f"cross{clip}_bps"
        for s, g in pair.groupby("sym", sort=True):
            g = g.sort_values("sec")
            on = g[col].values > 0
            secs = g.sec.values
            vals = g[col].values
            i = 0
            while i < len(g):
                if not on[i]:
                    i += 1
                    continue
                j = i
                while j + 1 < len(g) and on[j + 1] and secs[j + 1] - secs[j] <= BRIDGE_S:  # a missing second is a recorder hole, bridged
                    j += 1
                rows.append((clip, s, secs[i], secs[j] + 1, secs[j] + 1 - secs[i], float(vals[i : j + 1].max()), float(vals[i : j + 1].mean())))
                i = j + 1
    return pd.DataFrame(rows, columns=["clip_usd", "sym", "start", "end", "duration_s", "max_bps", "mean_bps"])


def merge() -> None:
    tabs: dict[str, dict[str, pd.DataFrame]] = {}
    for kind in ("bbo", "trades", "stats", "grid"):
        for venue in ("lighter", "rh"):
            parts = sorted(PARTS.glob(f"{venue}_*_{kind}.parquet"))
            df = pd.concat([pd.read_parquet(p) for p in parts], ignore_index=True).sort_values("t" if kind != "grid" else "sec", kind="stable")
            df.to_parquet(OUT / f"{kind}_{venue}.parquet", index=False)
            tabs.setdefault(kind, {})[venue] = df
            print(f"{kind}_{venue}: {len(df)} rows", flush=True)
    gl, gr = tabs["grid"]["lighter"].add_prefix("l_"), tabs["grid"]["rh"].add_prefix("r_")
    pair = gl.merge(gr, left_on=["l_sec", "l_sym"], right_on=["r_sec", "r_sym"], how="inner").rename(columns={"l_sec": "sec", "l_sym": "sym"}).drop(columns=["r_sec", "r_sym"])
    l_mid, r_mid = (pair.l_bid + pair.l_ask) / 2, (pair.r_bid + pair.r_ask) / 2
    pair["gap_bps"] = (l_mid / r_mid - 1) * 1e4  # Lighter mid over RH mid
    pair["l_spread_bps"] = (pair.l_ask / pair.l_bid - 1) * 1e4
    pair["r_spread_bps"] = (pair.r_ask / pair.r_bid - 1) * 1e4
    pair["cross_touch_bps"] = pd.concat([pair.l_bid / pair.r_ask - 1, pair.r_bid / pair.l_ask - 1], axis=1).max(axis=1) * 1e4  # > 0: touches crossed
    for c in CLIPS:  # sell into one venue's bids for `c` USD, buy from the other's asks: the edge a taker-taker round trip would earn
        pair[f"cross{c}_bps"] = pd.concat([pair[f"l_sell{c}"] / pair[f"r_buy{c}"] - 1, pair[f"r_sell{c}"] / pair[f"l_buy{c}"] - 1], axis=1).max(axis=1) * 1e4
    pair.to_parquet(OUT / "pair_1s.parquet", index=False)
    print(f"pair_1s: {len(pair)} rows", flush=True)

    ep_t = episodes_touch(tabs["bbo"]["lighter"], tabs["bbo"]["rh"])
    ep_t.to_csv(OUT / "cross_episodes_touch.csv", index=False)
    ep_s = episodes_sized(pair)
    ep_s.to_csv(OUT / "cross_episodes_sized.csv", index=False)
    print(f"episodes: touch {len(ep_t)} (over 5 s: {(ep_t.duration_s > 5).sum()}), sized {len(ep_s)}", flush=True)

    vol = {v: tabs["stats"][v].groupby("sym").volume_usd.median() for v in ("lighter", "rh")}
    fund = {v: tabs["stats"][v].groupby("sym").funding.mean() for v in ("lighter", "rh")}
    mins = {}
    for v in ("lighter", "rh"):  # one-minute means of index and mark per venue, so the two venues' references can be compared
        st = tabs["stats"][v].copy()
        st["minute"] = (st.t // 60).astype(int)
        mins[v] = st.groupby(["sym", "minute"])[["index", "mark"]].mean()
    ref = mins["lighter"].join(mins["rh"], lsuffix="_l", rsuffix="_r", how="inner")
    ref["index_gap_bps"] = (ref.index_l / ref.index_r - 1) * 1e4
    ref["mark_gap_bps"] = (ref.mark_l / ref.mark_r - 1) * 1e4
    ref = ref.groupby(level="sym")[["index_gap_bps", "mark_gap_bps"]].median()
    rows = []
    for s, g in pair.groupby("sym"):
        e = ep_t[ep_t.sym == s]
        rows.append({
            "sym": s,
            "seconds": len(g),
            "hours": len(g) / 3600,
            "gap_mean_bps": g.gap_bps.mean(),
            "gap_std_bps": g.gap_bps.std(),
            "gap_p05_bps": g.gap_bps.quantile(0.05),
            "gap_p50_bps": g.gap_bps.median(),
            "gap_p95_bps": g.gap_bps.quantile(0.95),
            "pct_abs_gap_over_20bps": (g.gap_bps.abs() > 20).mean() * 100,
            "pct_abs_gap_over_50bps": (g.gap_bps.abs() > 50).mean() * 100,
            "l_spread_p50_bps": g.l_spread_bps.median(),
            "r_spread_p50_bps": g.r_spread_bps.median(),
            "pct_touch_crossed": (g.cross_touch_bps > 0).mean() * 100,
            "pct_cross100_over_0": (g.cross100_bps > 0).mean() * 100,
            "pct_cross100_over_10bps": (g.cross100_bps > 10).mean() * 100,
            "pct_cross500_over_10bps": (g.cross500_bps > 10).mean() * 100,
            "pct_cross2000_over_10bps": (g.cross2000_bps > 10).mean() * 100,
            "cross100_p99_bps": g.cross100_bps.quantile(0.99),
            "touch_episodes": len(e),
            "touch_episodes_over_5s": int((e.duration_s > 5).sum()),
            "touch_episodes_over_60s": int((e.duration_s > 60).sum()),
            "touch_crossed_seconds": e.duration_s.sum(),
            "longest_touch_episode_s": e.duration_s.max() if len(e) else 0.0,
            "max_touch_cross_bps": e.max_bps.max() if len(e) else 0.0,
            "index_gap_p50_bps": ref.index_gap_bps.get(s, NAN),  # Lighter index over RH index: a reference difference, not a book one
            "mark_gap_p50_bps": ref.mark_gap_bps.get(s, NAN),
            "l_funding_mean": fund["lighter"].get(s, NAN),
            "r_funding_mean": fund["rh"].get(s, NAN),
            "l_volume_usd_24h": vol["lighter"].get(s, NAN),
            "r_volume_usd_24h": vol["rh"].get(s, NAN),
        })
    summ = pd.DataFrame(rows).sort_values("pct_abs_gap_over_20bps", ascending=False)
    summ.to_csv(OUT / "summary_by_market.csv", index=False, float_format="%.3f")

    cov = []
    for venue in ("lighter", "rh"):
        b = tabs["bbo"][venue]
        b["hour"] = pd.to_datetime(b.t, unit="s").dt.floor("h")
        for h, g in b.groupby("hour"):
            cov.append({"venue": venue, "hour_utc": h, "bbo_rows": len(g), "reconnects": int(g.bid.isna().sum() / max(1, g.sym.nunique()))})
    pd.DataFrame(cov).to_csv(OUT / "coverage.csv", index=False)
    print("wrote summary_by_market.csv and coverage.csv", flush=True)


if __name__ == "__main__":
    if sys.argv[1] == "hour":
        hour(Path(sys.argv[2]))
    else:
        merge()
