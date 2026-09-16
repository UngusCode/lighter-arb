"""Liquidation guard. Every position is one leg of a hedged pair across two accounts, so a big move drains one
account while it fills the other; the risk is a leg being liquidated, never the pair's direction. Steer by the one
number the venue acts on, its own liquidation price per position:

buffer  = |liquidation price / mark - 1|, the adverse move that liquidates; none reported = out of reach
top up  = an isolated position under `liq_buffer` gets margin added from the account's free balance, back to twice
          the buffer: one transaction, no trading cost
shrink  = under half of `liq_buffer`, close a fraction f = (half - buffer) / half of the pair every second, both
          legs by IOC at once: no naked leg, the cost is the two spreads. As the pair shrinks the buffer recovers
          and f falls to zero; at buffer zero the whole pair goes.
Entries stop while shrinking. Nothing happens above the threshold, so the edge is untouched in normal conditions."""

from __future__ import annotations

import time

from .log import Log
from .venue import Venue

log = Log("guard")
TOP_UP_EVERY_S = 60.0


class Guard:
    def __init__(self, liq_buffer: float) -> None:
        self.b = liq_buffer
        self._topped: dict[tuple[str, str], float] = {}  # (venue, symbol) -> when margin was last added

    def shrink(self, buffer: float | None) -> float:
        """Fraction of the pair to close this second: zero above half the buffer, all of it at zero."""
        hard = self.b / 2
        return 0.0 if buffer is None or buffer >= hard else min(1.0, (hard - buffer) / hard)

    async def step(self, symbol: str, buffers: dict[Venue, float | None], notional_usd: float) -> float:
        """Top up what can be topped up; return the fraction of the pair to shrink now (the worst leg decides)."""
        f = 0.0
        for v, b in buffers.items():
            if b is not None and b < self.b and v.isolated(symbol) and time.time() - self._topped.get((v.name, symbol), 0) > TOP_UP_EVERY_S:
                add = min((2 * self.b - b) * notional_usd, v.free())
                if add > 0:
                    self._topped[(v.name, symbol)] = time.time()
                    log.warning("guard.top_up", venue=v.name, symbol=symbol, buffer=round(b, 3), add_usd=round(add, 2))
                    await v.add_margin(symbol, add)
            f = max(f, self.shrink(b))
        return f
