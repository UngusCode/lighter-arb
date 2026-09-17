"""Market loops, hedging, risk, CLI."""

from __future__ import annotations

import argparse
import asyncio
import signal
import time
from collections.abc import Coroutine
from pathlib import Path
from typing import Any

from .config import Config, load
from .guard import Guard
from .log import Log, setup
from .strategy import Average, Top, cross, quotes
from .venue import Venue

log = Log("bot")
HEDGE_WAIT_S = 5.0  # an IOC is in flight until every venue it went to shows its position move, or this long: fill reports can lag
STATUS_EVERY_S = 60
DUST_USD = 1.0  # unhedged notional below this is left alone; a taker order can be any size, so this is only about not spamming
ENTRY_PAUSE_S = 2.0  # no new entry on a market this soon after a fill there: the position it is sized against must include the fill
HEDGE_TRIES = 3  # hedge IOCs that may leave a mismatch standing before it is undone on the quote venue instead
HEDGE_SLIP_BPS = 50.0  # a hedge IOC is limited this far past the touch: a cap, it still fills at the best available
MAX_BOOK_AGE_MS = 1500.0  # older books on either venue pull the quotes
HALT_FILE = "HALT"  # touch it to halt
SNAPSHOT_WAIT_S = 30.0  # startup gives the venues this long to send their account snapshots


class Bot:
    def __init__(self, cfg: Config) -> None:
        self.cfg, self.c = cfg, cfg.strategy
        self.q, self.h = Venue("quote", cfg.venues["quote"], cfg.leverage), Venue("hedge", cfg.venues["hedge"], cfg.leverage)
        self.guard = Guard(cfg.liq_buffer)
        self.shrunk: dict[str, float] = {}  # symbol -> when the guard last shrank the pair
        self.equity0: float | None = None  # combined equity at the first reading; the drawdown halt measures from here
        self.selected = set(cfg.markets)  # the markets traded; a position anywhere else is inherited: only reduced, and its loop stops once flat
        self.loops: dict[str, asyncio.Task[None]] = {}
        self.sent: dict[str, tuple[float, dict[Venue, float]]] = {}  # symbol -> (when, position on each venue an IOC went to) of the last IOCs
        self.hedge_tries: dict[str, int] = {}  # symbol -> hedge IOCs sent for the mismatch that is still standing
        self._state: dict[str, dict] = {}  # latest numbers per market, for the status line
        self.halted: str | None = None
        self.stop = asyncio.Event()

    async def guarded(self, name: str, coro: Coroutine[Any, Any, None]) -> None:
        """Every long-running task: a crash anywhere halts the bot rather than dying quietly."""
        try:
            await coro
        except Exception as e:
            log.error("loop.crashed", loop=name, err=repr(e))
            await self.halt("exception")

    # ----- one market: rest on the quote venue, hedge every fill on the hedge venue -----
    async def market_loop(self, symbol: str) -> None:
        making = self.c.mode == "make"
        premium = Average(self.c.premium_tau_s, self.cfg.markets.get(symbol))  # the thin venue's premium over the deep one, bps
        if making:
            await self.q.watch(symbol)  # our orders on this market, from the venue
        while not self.stop.is_set() and not self.halted:
            await asyncio.sleep(0.1)
            qt, ht = self.q.top(symbol), self.h.top(symbol)
            if not qt or not ht or max(qt.age_ms, ht.age_ms) > MAX_BOOK_AGE_MS:
                await self.q.sync(symbol, {})  # a stale side: nothing rests until both books are current
                continue
            pos, mid, qmid = self.q.position(symbol), (ht.bid + ht.ask) / 2, (qt.bid + qt.ask) / 2
            gap = (qmid / mid - 1) * 1e4
            premium.update(gap, time.time())
            want = quotes(qt, ht, pos, self.c, self.entry_gap(symbol), premium.value or 0.0) if making else []
            mismatch = pos + self.h.position(symbol)  # base units not hedged
            buffers = {self.q: self.q.buffer(symbol, qmid), self.h: self.h.buffer(symbol, mid)}
            shrink = await self.guard.step(symbol, buffers, abs(pos) * mid)
            if shrink and pos and time.time() - self.shrunk.get(symbol, 0) >= 1:  # too close to liquidation: both legs off, at once
                self.shrunk[symbol] = time.time()
                log.warning("guard.shrink", symbol=symbol, fraction=round(shrink, 3), buffers={v.name: b for v, b in buffers.items()})
                await self.ioc(symbol, [(self.q, pos > 0, abs(pos) * shrink, None), (self.h, pos < 0, abs(pos) * shrink, None)])
            short = min(self.q.free(), self.h.free()) < 2 * self.c.clip_usd / self.cfg.leverage  # a clip on each venue, with a second behind it
            if symbol not in self.selected or not premium.warm(time.time()) or shrink or time.time() - self.q.last_fill.get(symbol, 0) < ENTRY_PAUSE_S or short:
                # inherited, average still warming, shrinking, just filled (let the position catch up), or a venue short of margin: only the reducing side
                want = [w for w in want if pos and (pos > 0) == w.is_ask]
                if not pos and abs(mismatch) * mid < DUST_USD and not self.q.resting(symbol) and symbol not in self.selected:
                    break
            self._state[symbol] = {
                "gap": round(gap, 1),
                "premium": round(premium.value or 0.0, 1) if premium.warm(time.time()) else None,
                "orders": len(self.q.resting(symbol)),
                "buffer": min((round(b, 3) for b in buffers.values() if b is not None), default=None),
                "inv_usd": round(pos * mid, 1),
                "unhedged_usd": round(mismatch * mid, 2),
            }
            if abs(mismatch) * mid < DUST_USD:
                self.hedge_tries.pop(symbol, None)
                if not making and not self.in_flight(symbol) and not shrink and not short and symbol in self.selected:
                    await self.take_cross(symbol, qt, ht, pos, mid)
            elif not self.in_flight(symbol):
                tries = self.hedge_tries[symbol] = self.hedge_tries.get(symbol, 0) + 1
                v = self.h if tries <= HEDGE_TRIES else self.q  # the hedge venue will not take it (margin, depth): undo the fill instead
                if v is self.q:
                    self.hedge_tries.pop(symbol, None)
                    log.warning("hedge.undo", symbol=symbol, unhedged_usd=round(mismatch * mid, 2))
                await self.ioc(symbol, [(v, mismatch > 0, abs(mismatch), None)])
            await self.q.sync(symbol, {w.is_ask: w for w in want})
        await self.q.sync(symbol, {})
        if making:
            await self.q.watch(symbol, on=False)
        log.info("market.stopped", symbol=symbol)

    async def ioc(self, symbol: str, legs: list[tuple[Venue, bool, float, float | None]]) -> None:
        """IOCs on one or both venues at once, as (venue, is_ask, size, limit). Nothing else goes out on this market until every
        venue sent to shows its position move, or HEDGE_WAIT_S: fill reports can lag, and one leg's report can land before the other's."""
        self.sent[symbol] = (time.time(), {v: v.position(symbol) for v, *_ in legs})
        await asyncio.gather(*(v.take(symbol, is_ask, size, HEDGE_SLIP_BPS, limit) for v, is_ask, size, limit in legs))

    def in_flight(self, symbol: str) -> bool:
        when, before = self.sent.get(symbol, (0.0, {}))
        return time.time() - when < HEDGE_WAIT_S and any(v.position(symbol) == p for v, p in before.items())

    async def take_cross(self, symbol: str, qt: Top, ht: Top, pos: float, mid: float) -> None:
        """Cross mode: where one venue's bids sit over the other's asks by more than both taker fees and the minimum edge, take
        both sides at once for the size the books cross by, within a clip and the inventory cap. A leg the other venue did not
        fill in full is squared by the hedge logic like any other mismatch."""
        need = self.c.cross_min_edge_bps + self.q.cfg.taker_fee_bps + self.h.cfg.taker_fee_bps
        if qt.bid < ht.ask * (1 + need / 1e4) and ht.bid < qt.ask * (1 + need / 1e4):
            return  # not even the touches cross by enough: no walk
        a = cross(self.q.side(symbol, "bids"), self.h.side(symbol, "asks"), need)  # sell the quote venue, buy the hedge venue
        b = cross(self.h.side(symbol, "bids"), self.q.side(symbol, "asks"), need)  # the other way round
        sell_q, (size, bid_lim, ask_lim, edge) = (True, a) if a[0] * a[3] >= b[0] * b[3] else (False, b)
        room = self.c.max_inventory_usd / mid + (pos if sell_q else -pos)  # what the cap still allows in this direction
        size = min(size, self.c.clip_usd / mid, room)
        if size * mid < DUST_USD:
            return
        log.info("cross", symbol=symbol, side="sell quote, buy hedge" if sell_q else "buy quote, sell hedge", size=round(size, 6), edge_bps=round(edge, 1))
        await self.ioc(symbol, [(self.q, sell_q, size, bid_lim if sell_q else ask_lim), (self.h, not sell_q, size, ask_lim if sell_q else bid_lim)])

    def entry_gap(self, symbol: str) -> float | None:
        """The gap a held pair was put on at, from the venues' average entry prices; None unless both legs are on."""
        qa, ha = self.q.avg_entry.get(symbol), self.h.avg_entry.get(symbol)
        return (qa / ha - 1) * 1e4 if qa and ha and self.q.position(symbol) * self.h.position(symbol) < 0 else None

    def live(self) -> set[str]:
        return {s for s, t in self.loops.items() if not t.done()}

    # ----- risk -----
    async def halt(self, reason: str) -> None:
        """Cancel everything and exit non-zero: positions stay as they are, and the service shows as failed until a human looks."""
        if not self.halted:
            self.halted = reason
            log.error("HALT", reason=reason)
            for v in (self.q, self.h):
                await v.cancel_all()
            self.stop.set()

    async def risk_loop(self) -> None:
        next_status = time.time() + STATUS_EVERY_S
        while not self.stop.is_set() and not self.halted:
            await asyncio.sleep(1)
            eq = [v.equity for v in (self.q, self.h)]
            equity = sum(eq) if all(e is not None for e in eq) else None  # type: ignore[arg-type]
            if equity is not None and self.equity0 is None:
                self.equity0 = equity
            if time.time() >= next_status:  # one status line a minute: the whole state, greppable
                next_status += STATUS_EVERY_S
                log.info(
                    "status",
                    equity=equity,
                    free={v.name: round(v.free(), 1) for v in (self.q, self.h)},
                    since_start=None if equity is None or self.equity0 is None else round(equity - self.equity0, 2),
                    markets={s: st for s, st in self._state.items() if s in self.live()},
                )
            if await asyncio.to_thread(Path(HALT_FILE).exists):
                await self.halt("halt_file")
            elif equity is not None and self.equity0 is not None and equity - self.equity0 < -self.cfg.drawdown_usd:
                await self.halt("drawdown")

    async def run(self) -> None:
        for v in (self.q, self.h):
            await v.load()
        shared = set(self.q.markets) & set(self.h.markets)
        if missing := self.selected - shared:
            raise RuntimeError(f"not listed on both venues: {sorted(missing)}")
        for v in (self.q, self.h):
            await v.start(self.selected)
        tasks = [asyncio.create_task(v.run_ws()) for v in (self.q, self.h)]
        for _ in range(int(SNAPSHOT_WAIT_S * 10)):  # the account snapshots: positions and collateral, before anything is decided on them
            if all(v.equity is not None for v in (self.q, self.h)):
                break
            await asyncio.sleep(0.1)
        else:
            raise RuntimeError(f"no account snapshot from both venues within {SNAPSHOT_WAIT_S:.0f} s")
        held = {s for v in (self.q, self.h) for s in v.markets if v.position(s)}
        for v in (self.q, self.h):
            await v.cancel_all()  # a clean slate: nothing of ours rests that this process does not know about
            await v.track(held & shared)  # books for what is held too
        for s in (held & shared) - self.selected:  # inherited positions: only reduced
            self.loops[s] = asyncio.create_task(self.guarded(s, self.market_loop(s)))
            log.info("market.adopt", symbol=s, quote_pos=self.q.position(s), hedge_pos=self.h.position(s))
        for s, seed in self.cfg.markets.items():
            for v in (self.q, self.h):
                await v.enter(s, self.cfg.leverage)
            self.loops[s] = asyncio.create_task(self.guarded(s, self.market_loop(s)))
            log.info("market.enter", symbol=s, premium_seed_bps=seed, quote_pos=self.q.position(s), hedge_pos=self.h.position(s))
        tasks.append(asyncio.create_task(self.guarded("risk", self.risk_loop())))
        log.info("running", quote=self.cfg.venues["quote"].base_url, hedge=self.cfg.venues["hedge"].base_url, mode=self.c.mode, markets=list(self.cfg.markets))
        try:
            await self.stop.wait()
        finally:
            for v in (self.q, self.h):
                await v.cancel_all()  # while the sockets are still up
            for t in tasks + list(self.loops.values()):
                t.cancel()
            for v in (self.q, self.h):
                await v.close()


def cli() -> None:
    ap = argparse.ArgumentParser(prog="lighter-arb")
    ap.add_argument("--config", default="config.yaml")
    cfg = load(ap.parse_args().config)
    setup()

    async def main() -> None:
        bot = Bot(cfg)  # SDK clients must be built inside the loop
        for sig in (signal.SIGINT, signal.SIGTERM):
            asyncio.get_running_loop().add_signal_handler(sig, bot.stop.set)
        await bot.run()  # run() cancels all on both venues in its finally, whatever the exit reason
        if bot.halted:
            raise SystemExit(1)

    asyncio.run(main())
