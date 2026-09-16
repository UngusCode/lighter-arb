"""Make the thin venue's spread around its premium, hedged on the deep venue. Pure.

premium   = a moving average of the thin venue's mid over the deep venue's, in bps; zero means parity
entry ask = max(hedge ask x (1 + max(premium, 0) + edge), own ask)   opening a short: above the premium, never below parity
entry bid = min(hedge bid x (1 + min(premium, 0) - edge), own bid)   opening a long: below the discount, never above parity
The average only ever pushes an entry away from parity, never across it: an entry never buys on the dearer venue or
sells on the cheaper one. A rich market gets asks only, a cheap one bids only.
exit      = the pair's own entry gap less twice the edge, so the round trip nets the edge, sized to what is held
Never inside the thin venue's own touch: a resting order there is filled by takers crossing the thin venue's whole
spread, and that spread is the edge. Every fill is hedged at once on the deep venue.
"""

from __future__ import annotations

import math
from collections import deque
from typing import NamedTuple

from .config import StrategyCfg


class Top(NamedTuple):
    bid: float
    ask: float
    age_ms: float


type Side = list[tuple[float, float]]  # one side of a book as (price, size), best first


class Quote(NamedTuple):
    is_ask: bool
    price: float
    size: float


class Average:
    """Exponential moving average with time constant tau. Seeded, it is warm at once; unseeded, once it has run for tau."""

    def __init__(self, tau_s: float, seed: float | None) -> None:
        self.tau, self.value, self.seeded = tau_s, seed, seed is not None
        self._since: float | None = None
        self._t = 0.0

    def update(self, x: float, now: float) -> None:
        if self.value is None:
            self.value, self._since = x, now
        else:
            self.value += (1 - math.exp(-(now - self._t) / self.tau)) * (x - self.value)
        self._t = now

    def warm(self, now: float) -> bool:
        return self.seeded or (self._since is not None and now - self._since >= self.tau)


def cross(bids: Side, asks: Side, min_bps: float) -> tuple[float, float, float, float]:
    """Cross mode. Match the highest bids of one venue with the lowest asks of the other for as long as the bid is at least
    min_bps over the ask: (size matched, deepest bid taken, deepest ask taken, size-weighted edge in bps). Selling that size
    into the first book at its deepest bid and buying it from the second at its deepest ask clears the edge."""
    bids, asks = deque(bids), deque(asks)
    size = gain = bid = ask = 0.0
    while bids and asks and bids[0][0] >= asks[0][0] * (1 + min_bps / 1e4):
        (bid, bq), (ask, aq) = bids[0], asks[0]
        q = min(bq, aq)
        size += q
        gain += q * (bid - ask)
        bids[0], asks[0] = (bid, bq - q), (ask, aq - q)
        if bq <= q:
            bids.popleft()
        if aq <= q:
            asks.popleft()
    return (size, bid, ask, gain / (size * ask) * 1e4) if size > 0 else (0.0, 0.0, 0.0, 0.0)


def quotes(q: Top, h: Top, pos: float, c: StrategyCfg, entry_bps: float | None, premium_bps: float = 0.0) -> list[Quote]:
    """Quotes to rest on the thin venue. pos: position held there; entry_bps: the gap the held pair was put on at
    (thin venue entry over deep venue entry), None when unknown, in which case the exit is priced like an entry;
    premium_bps: the average of the thin venue's mid over the deep venue's, which entries rest either side of."""
    mid = (h.bid + h.ask) / 2
    clip, inv_usd, e, m = c.clip_usd / mid, pos * mid, c.edge_bps / 1e4, premium_bps / 1e4
    up, down = max(m, 0.0), min(m, 0.0)  # the premium moves the ask up or the bid down, never a quote across parity
    out = []
    room = max(0.0, c.max_inventory_usd - abs(inv_usd)) / mid  # an entry is sized to what fits under the cap, at most a clip
    if pos > 0:  # closing a long, bought below the deep venue: sell once above the entry gap plus the edge
        k = 1 + (entry_bps / 1e4 + 2 * e if entry_bps is not None else up + e)
        out.append(Quote(True, max(h.ask * k, q.ask), min(clip, pos)))
    elif room > 0:  # opening a short: above the premium by the edge, and above the deep venue whatever the premium
        out.append(Quote(True, max(h.ask * (1 + up + e), q.ask), min(clip, room)))
    if pos < 0:  # closing a short, sold above the deep venue: buy once below the entry gap less the edge
        k = 1 + (entry_bps / 1e4 - 2 * e if entry_bps is not None else down - e)
        out.append(Quote(False, min(h.bid * k, q.bid), min(clip, -pos)))
    elif room > 0:  # opening a long: below the discount by the edge, and below the deep venue whatever the premium
        out.append(Quote(False, min(h.bid * (1 + down - e), q.bid), min(clip, room)))
    return out
