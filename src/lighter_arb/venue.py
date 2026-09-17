"""One Lighter instance (mainnet, testnet, or Robinhood Chain).

The SDK does the socket, book deltas, signing, nonces and REST. This adds only what the SDK has no notion of:
market rounding and minimums, which side of a trade is ours, which of our orders are resting, pushing signed
transactions over the socket, and the cancel-all."""

from __future__ import annotations

import asyncio
import contextlib
import json
import math
import time
from collections.abc import Callable
from typing import Any, NamedTuple

from lighter.api.order_api import OrderApi
from lighter.api_client import ApiClient
from lighter.configuration import Configuration
from lighter.nonce_manager import NonceManagerType
from lighter.signer_client import SignerClient
from lighter.ws_client import WsClient
from websockets.client import connect as connect_async  # type: ignore[attr-defined]  # what the SDK itself uses

from .config import VenueCfg
from .log import Log
from .strategy import Quote, Side, Top

log = Log("venue")
ORDER_EXPIRY_S = 300  # resting orders carry the venue-minimum expiry and are re-created a minute before it: the venue clears the book by itself if we die
REQUOTE_TOL_BPS = 10.0  # a resting quote is left alone unless its target moved more than this: every modify costs volume quota
PING_EVERY_S = 20.0  # the venue's application-level keepalive: we ping, it pongs
SILENT_S = 60.0  # a socket with no message for this long is dead: close it and reconnect
SQUEEZE_S = 60.0  # after a margin reject, how long the venue is taken to have no free balance: no creates until then
PAUSE_S = 60.0  # after a rate-limit reject, how long nothing is quoted on the venue: resting orders cancelled (free), no creates or modifies
PACE_S = 16.0  # at most one create or modify per venue this often: the venue gives one free transaction every 15 s, so no volume quota is spent
SNAPSHOT_GRACE_S = 3.0  # an order sent this recently may not be in an account snapshot yet: the snapshot does not drop it
CANCEL_ALL_ID = 0  # tx ids below 1 are ours for account-level txs; orders use client order indexes from a running counter
LEVERAGE_ID, MARGIN_ID = -1, -1_000_000  # minus the market id


class Order(NamedTuple):
    symbol: str
    coi: int  # client order index; also what we cancel and modify by
    is_ask: bool
    price: float
    size: float
    placed_at: float


class Market(NamedTuple):
    id: int
    price_dec: int
    size_dec: int
    min_base: float
    min_quote: float  # minimum order notional


class _Ws(WsClient):
    """The SDK client plus what it lacks: the venue's answers to the txs we push, the authenticated per-market account channel
    that carries our orders' lifecycle, subscriptions that are held rather than fired once, and a patient keepalive."""

    def __init__(
        self, on_tx_response: Callable[[dict], None], on_market: Callable[[dict], None], on_account: Callable[[dict], None], auth: Callable[[], str], **kw: Any
    ) -> None:
        super().__init__(**kw)
        self.on_tx_response, self.on_market, self.on_account, self.auth = on_tx_response, on_market, on_account, auth
        self.wanted: set[str] = set()  # channels to hold, as the venue names them in messages: order_book:1, account_all:2, account_market:1:2
        self.alive: dict[str, float] = {}  # channel -> when it last spoke on this connection (or was requested)
        self.last_msg = 0.0  # any message proves the socket is alive; a thin book can be unchanged and still current

    async def on_message_async(self, ws: Any, message: Any) -> None:
        msg = json.loads(message)
        self.last_msg = time.time()
        if ch := msg.get("channel"):
            self.alive[ch] = self.last_msg
        if msg.get("type") == "connected":
            await self.handle_connected_async(ws)
        elif msg.get("type") == "ping":
            await ws.send('{"type":"pong"}')
        elif msg.get("type") == "update/order_book" and self._bookless(ch or ""):
            return  # ahead of its snapshot, which supersedes it
        elif str(msg.get("type", "")).endswith("/account_all"):
            self.on_account(msg)
        else:
            self.on_message(ws, msg)

    async def handle_connected_async(self, ws: Any) -> None:  # noqa: ARG002  # the SDK's signature; the socket is self.ws
        """A fresh connection: no book from before the drop survives it (nothing rests on a market until its snapshot is back),
        and every held channel is requested again. The venue can silently drop some of a burst of requests, so the keepalive
        re-requests any channel that has not spoken since."""
        self.order_book_states.clear()
        self.alive = dict.fromkeys(self.wanted, time.time())
        await self.subscribe(self.wanted)

    def _bookless(self, ch: str) -> bool:
        """A held book channel whose snapshot has not arrived (or was discarded), whatever else it sends."""
        return ch.startswith("order_book:") and ch.split(":")[1] not in self.order_book_states

    async def subscribe(self, channels: set[str], on: bool = True) -> None:
        for ch in channels:
            msg = {"type": "subscribe" if on else "unsubscribe", "channel": ch.replace(":", "/")}
            if ch.startswith("account_market"):
                msg["auth"] = self.auth()
            with contextlib.suppress(Exception):  # a dead socket: the next connection requests everything wanted
                await self.ws.send(json.dumps(msg))  # type: ignore[union-attr]

    def handle_unhandled_message(self, message: Any) -> None:  # type: ignore[override]  # SDK leaves it untyped
        if "id" in message:
            self.on_tx_response(message)
        elif str(message.get("type", "")).endswith("/account_market"):
            self.on_market(message)

    async def run_async(self) -> None:
        """The SDK's loop with the venue's keepalive: the venue expects the client to send {"type": "ping"} and answers
        {"type": "pong"}; it does not reliably answer WebSocket ping frames, so those are off. A socket silent for
        SILENT_S is closed here, which ends the loop and lets the reconnect loop replace it."""
        self.ws = await connect_async(self.base_url, ping_interval=None)
        keepalive = asyncio.create_task(self._keepalive(self.ws))
        try:
            async for message in self.ws:
                await self.on_message_async(self.ws, message)
        finally:
            keepalive.cancel()

    async def _keepalive(self, ws: Any) -> None:
        while True:
            await asyncio.sleep(PING_EVERY_S)
            now = time.time()
            if now - self.last_msg > SILENT_S:
                await ws.close()
                return
            await ws.send('{"type":"ping"}')
            stale = {ch for ch in self.wanted if now - self.alive.get(ch, 0) > SILENT_S or self._bookless(ch) or ch.startswith("account")}
            await self.subscribe(stale, on=False)  # the venue refuses a second subscribe as "already subscribed" and sends no snapshot
            await self.subscribe(stale)  # account channels every time: their snapshot is the reconcile, positions, collateral and orders


class Venue:
    def __init__(self, name: str, cfg: VenueCfg, leverage: int) -> None:
        self.name, self.cfg, self.leverage = name, cfg, leverage
        self.markets: dict[str, Market] = {}
        self._by_id: dict[str, str] = {}  # str(market_id) -> symbol, as the SDK keys its book states
        self._positions: dict[str, float] = {}
        self._raw: dict[str, dict] = {}  # symbol -> the venue's position record: unrealized pnl, allocated margin, margin fraction
        self.avg_entry: dict[str, float] = {}  # venue-reported average entry price per position; survives our restarts
        self._collateral: float | None = None  # the settlement asset's margin balance, from the account snapshot
        self._squeezed_until = 0.0  # after a margin reject: no free balance until then, whatever the snapshot says
        self._paused_until = 0.0  # after a rate-limit reject: nothing quoted until then
        self._next_order_tx = 0.0  # the pace: when the next create or modify may go out
        self._liq: dict[str, float] = {}  # symbol -> liquidation price the venue reports for the position, 0 when none
        self._isolated: dict[str, bool] = {}
        self._orders: dict[int, Order] = {}
        self._seen: set[int] = set()
        self.last_fill: dict[str, float] = {}  # symbol -> when we last saw a fill of ours there
        self._coi = int(time.time() * 1000) % 2**31  # unique per run, distinct from earlier runs
        self._last_nonce = 0
        self._pending: dict[int, tuple[str, Order | None]] = {}  # coi -> (kind, order before the send) for in-flight txs
        self._api = ApiClient(configuration=Configuration(host=cfg.base_url))
        self._signer = SignerClient(
            url=cfg.base_url,
            account_index=cfg.account_index,
            api_private_keys={cfg.api_key_index: cfg.api_key_private_key},
            nonce_management_type=NonceManagerType.NONE,
        )
        self._ws: _Ws | None = None

    # ----- lifecycle -----
    async def load(self) -> None:
        """Every perp the venue lists, so the two venues' lists can be intersected before anything is subscribed."""
        for ob in (await OrderApi(self._api).order_books()).order_books:
            if ob.market_type == "perp":
                self.markets[ob.symbol] = Market(
                    ob.market_id,
                    ob.supported_price_decimals,
                    ob.supported_size_decimals,
                    float(ob.min_base_amount),
                    float(ob.min_quote_amount or 0),
                )
                self._by_id[str(ob.market_id)] = ob.symbol

    async def start(self, symbols: set[str]) -> None:
        if err := self._signer.check_client():
            raise RuntimeError(f"{self.name}: signer check failed: {err}")
        self._ws = _Ws(
            self._on_tx_response,
            self._on_market,
            self._on_account,
            lambda: self._signer.create_auth_token_with_expiry()[0] or "",
            ws_url=self.cfg.base_url.replace("https://", "wss://") + "/stream",
            order_book_ids=[self.markets[s].id for s in symbols],  # the SDK wants its lists, though the channels are held below
            account_ids=[self.cfg.account_index],
            on_order_book_update=lambda *_: None,
            on_account_update=lambda *_: None,
        )
        self._ws.wanted = {f"order_book:{self.markets[s].id}" for s in symbols} | {f"account_all:{self.cfg.account_index}"}

    async def run_ws(self) -> None:
        """Keep the socket up. The reconnect delay doubles up to a minute while the venue keeps refusing us (a WAF
        challenge or an outage), so a long refusal does not become a flood of attempts; it resets once a connection lasts."""
        assert self._ws
        delay = 1.0
        while True:
            started = time.time()
            try:
                await self._ws.run_async()
            except Exception as e:
                log.warning("venue.reconnect", venue=self.name, err=repr(e)[:200], retry_s=delay)
            delay = 1.0 if time.time() - started > 60 else min(delay * 2, 60.0)
            await asyncio.sleep(delay)

    async def close(self) -> None:
        await self._signer.close()
        await self._api.close()

    # ----- state -----
    def side(self, symbol: str, name: str) -> Side:
        """One side of the book as (price, size), best first, less our own resting orders: the touch a quote may not rest inside,
        the mid the premium is measured from and the depth a taker order counts on are the rest of the market, not our own quote.
        The SDK keeps each side as a list of strings in arrival order."""
        m = self.markets[symbol]
        book = self._ws.order_book_states.get(str(m.id)) if self._ws else None
        mine: dict[int, float] = {}
        for o in self._orders.values():
            if o.symbol == symbol and o.is_ask == (name == "asks"):
                mine[round(o.price * 10**m.price_dec)] = mine.get(round(o.price * 10**m.price_dec), 0.0) + o.size
        levels = ((float(o["price"]), float(o["size"]) - mine.get(round(float(o["price"]) * 10**m.price_dec), 0.0)) for o in (book or {}).get(name, ()))
        return sorted(((p, q) for p, q in levels if q > 0), reverse=name == "bids")

    def top(self, symbol: str) -> Top | None:
        """The rest of the market's touch; None while either side is empty or the book is crossed, when nothing of ours rests."""
        bids, asks = self.side(symbol, "bids"), self.side(symbol, "asks")
        if not bids or not asks or bids[0][0] >= asks[0][0]:
            return None
        return Top(bids[0][0], asks[0][0], (time.time() - self._ws.last_msg) * 1000)

    def depth(self, symbol: str, is_ask: bool, within_bps: float) -> float:
        """Base size resting within `within_bps` of the touch on the side an order of ours would hit (asks if we buy)."""
        side = self.side(symbol, "bids" if is_ask else "asks")
        return sum(s for p, s in side if abs(p / side[0][0] - 1) * 1e4 <= within_bps) if side else 0.0

    def position(self, symbol: str) -> float:
        return self._positions.get(symbol, 0.0)

    def buffer(self, symbol: str, mid: float) -> float | None:
        """Adverse move from `mid` that would liquidate the position here, as a fraction of price; None when the venue reports no
        liquidation price (it cannot be liquidated with the collateral behind it) or there is no position."""
        liq = self._liq.get(symbol, 0.0)
        return abs(liq / mid - 1) if liq and self.position(symbol) else None

    @property
    def equity(self) -> float | None:
        """Collateral plus allocated margin and unrealized pnl, as the venue counts it; None until the first account snapshot."""
        if self._collateral is None:
            return None
        return self._collateral + sum(float(p["allocated_margin"] or 0) + float(p["unrealized_pnl"] or 0) for p in self._raw.values())

    def free(self) -> float:
        """Free balance an entry may count on: collateral plus unrealized pnl, less the initial margin of every cross position and of
        what every resting order would add to one (the venue counts those too; the part that would reduce a position is free);
        nothing for a while after the venue rejected an order for margin."""
        if self._collateral is None or time.time() < self._squeezed_until:
            return 0.0
        im = sum(abs(float(p["position_value"])) * float(p["initial_margin_fraction"]) / 100 for p in self._raw.values() if not int(p["margin_mode"] or 0))
        opening = sum(max(0.0, o.size - max(0.0, self.position(o.symbol) * (1 if o.is_ask else -1))) * o.price for o in self._orders.values())
        return self._collateral + sum(float(p["unrealized_pnl"] or 0) for p in self._raw.values()) - im - opening / self.leverage

    async def track(self, symbols: set[str]) -> None:
        """Take a market's book as well (a position found at startup in a market not on the list)."""
        assert self._ws
        new = {f"order_book:{self.markets[s].id}" for s in symbols} - self._ws.wanted
        self._ws.wanted |= new
        await self._ws.subscribe(new)

    def isolated(self, symbol: str) -> bool:
        return self._isolated.get(symbol, False)

    async def add_margin(self, symbol: str, usd: float) -> None:
        """Move `usd` of free balance into an isolated position's margin."""
        m = self.markets[symbol].id
        log.info("margin.add", venue=self.name, symbol=symbol, usd=round(usd, 2))
        await self._send(
            self._sign(
                "sign_update_margin",
                MARGIN_ID - m,
                market_index=m,
                usdc_amount=int(usd * SignerClient.USDC_TICKER_SCALE),
                direction=SignerClient.ISOLATED_MARGIN_ADD_COLLATERAL,
            )
        )

    def resting(self, symbol: str) -> dict[bool, Order]:
        """Our live orders on this symbol keyed by is_ask: the oldest per side (any other is a surplus to cancel)."""
        out: dict[bool, Order] = {}
        for o in sorted((o for o in self._orders.values() if o.symbol == symbol), key=lambda o: o.placed_at):
            out.setdefault(o.is_ask, o)
        return out

    def surplus(self, symbol: str, is_ask: bool) -> list[Order]:
        keep = self.resting(symbol).get(is_ask)
        return [o for o in self._orders.values() if o.symbol == symbol and o.is_ask == is_ask and o is not keep]

    def tradable(self, symbol: str, price: float, size: float) -> bool:
        """Above both venue minimums for a resting order: base size and quote notional (takers have no minimum)."""
        m = self.markets[symbol]
        return size >= m.min_base and size * price >= m.min_quote

    async def enter(self, symbol: str, leverage: int) -> None:
        """A market enters (its book is already streaming): cross margin at `leverage` where the venue allows it, isolated
        where it insists (Lighter's pre-IPO markets). The market's details say which."""
        det = (await OrderApi(self._api).order_book_details(market_id=self.markets[symbol].id)).order_book_details[0]
        mode = SignerClient.ISOLATED_MARGIN_MODE if det.market_config.market_margin_mode else SignerClient.CROSS_MARGIN_MODE
        m = self.markets[symbol].id
        log.info("margin.set", venue=self.name, symbol=symbol, mode="isolated" if mode else "cross", leverage=leverage)
        await self._send(self._sign("sign_update_leverage", LEVERAGE_ID - m, market_index=m, fraction=int(10_000 / leverage), margin_mode=mode))

    # ----- inbound -----
    def _on_tx_response(self, msg: dict) -> None:
        """Venue's answer to a tx we pushed over the socket, matched by the client order id we sent as the id."""
        coi = int(msg["id"])
        if not msg.get("error"):
            self._pending.pop(coi, None)
            return
        log.warning("tx.reject", venue=self.name, id=msg["id"], err=msg["error"])
        if msg["error"].get("code") == 21739:  # not enough margin: whatever the last read said, there is none to spare for a while
            self._squeezed_until = time.time() + SQUEEZE_S
        elif msg["error"].get("code") == 23000:  # rate limited (volume quota): re-sending would only keep it exhausted
            self._paused_until = time.time() + PAUSE_S
        kind, before = self._pending.pop(coi, ("create", None))  # a rejected create never existed; a rejected modify or cancel leaves the original resting
        if kind == "create":
            self._orders.pop(coi, None)
        elif before is not None:
            self._orders[coi] = before

    async def watch(self, symbol: str, on: bool = True) -> None:
        """Follow (or stop following) our orders on this market over the venue's authenticated per-market channel."""
        assert self._ws
        ch = f"account_market:{self.markets[symbol].id}:{self.cfg.account_index}"
        (self._ws.wanted.add if on else self._ws.wanted.discard)(ch)
        await self._ws.subscribe({ch}, on)

    def _on_market(self, msg: dict) -> None:
        """The venue's view of our orders on one market: what rests, and what filled or was cancelled. Mirrored into _orders,
        so a resting order is one the venue has confirmed. Position and trades on this market come along with it."""
        s = self._by_id.get(msg["channel"].split(":")[1])
        if not s:
            return
        if msg["type"].startswith("subscribed"):  # a snapshot: whatever it does not list no longer rests, bar what was just sent
            for coi in [c for c, o in self._orders.items() if o.symbol == s and time.time() - o.placed_at > SNAPSHOT_GRACE_S]:
                self._orders.pop(coi)
        for o in msg.get("orders") or []:
            coi = int(o["client_order_index"])
            if o["status"] == "open":
                placed = self._orders[coi].placed_at if coi in self._orders else int(o["order_expiry"]) / 1000 - 300
                self._orders[coi] = Order(s, coi, o["is_ask"], float(o["price"]), float(o["remaining_base_amount"]), placed)
            elif o["status"] not in ("pending", "in-progress"):  # filled, or one of the cancelled statuses
                self._orders.pop(coi, None)
                self._pending.pop(coi, None)
        if p := msg.get("position"):
            self._set_position(s, p)
        self._fills(s, msg.get("trades") or [])

    def _set_position(self, symbol: str, p: dict) -> None:
        """A position as the venue reports it, with what the liquidation guard needs: its liquidation price and margin mode."""
        self._raw[symbol] = p
        self._positions[symbol] = float(p["sign"]) * float(p["position"])
        self.avg_entry[symbol] = float(p.get("avg_entry_price") or 0)
        self._liq[symbol] = float(p.get("liquidation_price") or 0)  # 0: the venue reports no price at which it liquidates
        self._isolated[symbol] = bool(int(p.get("margin_mode") or 0))

    def _fills(self, symbol: str, trades: list[dict]) -> None:
        me = self.cfg.account_index
        for tr in trades:
            sold, bought = tr.get("ask_account_id") == me, tr.get("bid_account_id") == me
            if int(tr["trade_id"]) in self._seen or not (sold or bought):
                continue
            self._seen.add(int(tr["trade_id"]))
            self.last_fill[symbol] = time.time()
            log.info("fill", venue=self.name, symbol=symbol, side="sell" if sold else "buy", price=tr["price"], size=tr["size"])

    def _on_account(self, msg: dict) -> None:
        """The account channel: a snapshot, on every (re)subscribe, replaces every position and the collateral; an update carries
        the positions that changed and the fills."""
        if msg["type"].startswith("subscribed"):
            self._raw.clear()
            self._positions.clear()
        for a in (msg.get("assets") or {}).values():
            if a.get("margin_mode") == "enabled":  # the settlement asset: USDC on one venue, USDG on the other
                self._collateral = float(a["margin_balance"])
        for mid, p in (msg.get("positions") or {}).items():
            if s := self._by_id.get(str(mid)):
                self._set_position(s, p)
        for mid, trades in (msg.get("trades") or {}).items():
            if s := self._by_id.get(str(mid)):
                self._fills(s, trades)

    # ----- outbound: one bid and one ask per market, pushed over the socket -----
    async def sync(self, symbol: str, want: dict[bool, Quote]) -> None:
        """No order -> create. Moved -> modify in place. Unwanted or near expiry -> cancel. Rate limited -> nothing rests for a while.
        Cancels are free and go at once; creates and modifies are paced to the venue's free transaction, one per PACE_S."""
        txs: list[tuple[int, str, int]] = []
        now = time.time()
        squeezed = now < self._squeezed_until  # the venue just refused an order for margin: no creates for a while, exits included
        if now < self._paused_until:
            want = {}
        for is_ask in (False, True):
            live = self.resting(symbol).get(is_ask)
            q = want.get(is_ask)
            for extra in self.surplus(symbol, is_ask):  # a second order on the same side (a snapshot race): cancel it
                self._orders.pop(extra.coi)
                txs += self._sign("sign_cancel_order", extra.coi, before=extra, market_index=self.markets[symbol].id, order_index=extra.coi)
            if live and q is None:
                self._orders.pop(live.coi)
                txs += self._sign("sign_cancel_order", live.coi, before=live, market_index=self.markets[symbol].id, order_index=live.coi)
                continue
            if q is None:
                continue
            expiring = live is not None and now - live.placed_at > ORDER_EXPIRY_S - 60
            if live and not expiring and abs(q.size / live.size - 1) <= 0.2 and abs(q.price / live.price - 1) * 1e4 <= REQUOTE_TOL_BPS:
                continue  # the resting order is inside the deadband
            if now < self._next_order_tx or (not live and squeezed):
                continue  # not yet: the order that rests (if any) stays as it is
            p_int, q_int, price, size = self._round(symbol, q.price, q.size, down=not is_ask)  # a tick toward passive
            if not self.tradable(symbol, price, size):
                continue
            if live and expiring:  # cancel (free) and re-create with a fresh expiry
                self._orders.pop(live.coi)
                txs += self._sign("sign_cancel_order", live.coi, before=live, market_index=self.markets[symbol].id, order_index=live.coi)
                live = None
            self._next_order_tx = now + PACE_S
            if live:
                self._orders[live.coi] = live._replace(price=price, size=size)
                txs += self._sign(
                    "sign_modify_order",
                    live.coi,
                    before=live,
                    market_index=self.markets[symbol].id,
                    order_index=live.coi,
                    base_amount=q_int,
                    price=p_int,
                    trigger_price=0,
                )
            else:
                self._coi += 1
                self._orders[self._coi] = Order(symbol, self._coi, is_ask, price, size, time.time())
                expiry = int(time.time() * 1000) + ORDER_EXPIRY_S * 1000
                txs += self._sign(
                    "sign_create_order", self._coi, **self._order(symbol, q_int, p_int, is_ask, SignerClient.ORDER_TIME_IN_FORCE_POST_ONLY, expiry)
                )
        await self._send(txs)

    async def take(self, symbol: str, is_ask: bool, size: float, max_slip_bps: float, limit: float | None = None) -> None:
        """An IOC. The hedge: limited at the touch plus the allowed slippage, never more than the book shows inside that.
        With `limit` given (cross mode): limited exactly there, for the size asked, since the caller walked the book itself."""
        if (t := self.top(symbol)) is None:
            return
        touch = t.bid if is_ask else t.ask
        if limit is None:
            limit = touch * (1 - max_slip_bps / 1e4) if is_ask else touch * (1 + max_slip_bps / 1e4)  # still fills if the touch moves a tick
            size = min(size, self.depth(symbol, is_ask, max_slip_bps))
        p_int, q_int, price, size = self._round(symbol, limit, size, down=is_ask)  # a tick toward aggressive
        if q_int <= 0:  # the venue's minimum sizes bind resting orders only: a taker can be any size the market's decimals allow
            return
        log.info("hedge", venue=self.name, symbol=symbol, side="sell" if is_ask else "buy", touch=touch, limit=price, size=size)
        self._coi += 1
        self._next_order_tx = time.time() + PACE_S  # a taker order takes the free transaction too
        await self._send(
            self._sign("sign_create_order", self._coi, **self._order(symbol, q_int, p_int, is_ask, SignerClient.ORDER_TIME_IN_FORCE_IMMEDIATE_OR_CANCEL, 0))
        )

    async def cancel_all(self) -> None:
        """Cancel everything now. If the process dies instead, every resting order expires on its own within ORDER_EXPIRY_S."""
        self._orders.clear()
        self._next_order_tx = time.time() + PACE_S  # a cancel-all takes the free transaction too
        await self._send(self._sign("sign_cancel_all_orders", CANCEL_ALL_ID, time_in_force=SignerClient.CANCEL_ALL_TIF_IMMEDIATE, timestamp_ms=0))

    # ----- implementation -----
    def _round(self, symbol: str, price: float, size: float, down: bool) -> tuple[int, int, float, float]:
        """Venue integer price and size, the price rounded down or up to the tick, the size down. Returns (p_int, q_int, price, size)."""
        m = self.markets[symbol]
        p_int = math.floor(price * 10**m.price_dec) if down else math.ceil(price * 10**m.price_dec)
        q_int = math.floor(size * 10**m.size_dec)
        return p_int, q_int, p_int / 10**m.price_dec, q_int / 10**m.size_dec

    def _nonce(self) -> dict[str, int]:
        """Skip-nonce mode: the venue takes any nonce above the last it saw on this key, so the clock is the counter and nothing
        is fetched, counted or re-fetched. Two signings in one millisecond still get distinct, rising nonces."""
        self._last_nonce = max(int(time.time() * 1000), self._last_nonce + 1)
        return {"skip_nonce": SignerClient.SKIP_NONCE_ON, "nonce": self._last_nonce, "api_key_index": self.cfg.api_key_index}

    def _sign(self, method: str, coi: int, before: Order | None = None, **kw: object) -> list[tuple[int, str, int]]:
        """Sign, remembering what to restore if the venue rejects it."""
        tx_type, tx_info, _, err = getattr(self._signer, method)(**kw, **self._nonce())
        if err:
            log.warning("sign.failed", venue=self.name, method=method, err=err)
            self._orders.pop(coi, None) if before is None else self._orders.__setitem__(coi, before)
            return []
        self._pending[coi] = (method.removeprefix("sign_").split("_")[0], before)  # create | modify | cancel | update
        return [(tx_type, tx_info, coi)]

    def _order(self, symbol: str, q_int: int, p_int: int, is_ask: bool, tif: int, expiry: int) -> dict[str, Any]:
        """Arguments of the SDK's sign_create_order: a plain limit order, keyed by our client order index."""
        return {"market_index": self.markets[symbol].id, "client_order_index": self._coi, "base_amount": q_int, "price": p_int, "is_ask": is_ask,
                "order_type": SignerClient.ORDER_TYPE_LIMIT, "time_in_force": tif, "reduce_only": False, "trigger_price": 0, "order_expiry": expiry}  # fmt: skip

    async def _send(self, txs: list[tuple[int, str, int]]) -> None:
        """Push signed txs over the socket one by one (only a single send can be the venue's free transaction) and move on: the
        venue's answer comes back by id and settles the local state then. A dead socket is not fatal: the tx counts as rejected
        so local state is restored, and the reconnect loop brings the feed back. An answer that never comes is settled by the
        next account snapshot."""
        for tx_type, tx_info, coi in txs:
            msg = {"type": "jsonapi/sendtx", "data": {"id": str(coi), "tx_type": tx_type, "tx_info": json.loads(tx_info)}}
            try:
                assert self._ws and self._ws.ws
                await self._ws.ws.send(json.dumps(msg))  # type: ignore[func-returns-value]  # SDK leaves the socket untyped
            except Exception as e:
                log.warning("send.failed", venue=self.name, err=repr(e))
                self._on_tx_response({"id": str(coi), "error": {"code": -1, "message": "send failed"}})
