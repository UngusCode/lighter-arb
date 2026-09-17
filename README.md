# lighter-arb

A reference bot for the two Lighter venues, Lighter mainnet and Lighter on Robinhood Chain, under a thousand lines of
Python on the official [lighter-python](https://github.com/elliottech/lighter-python) SDK. It trades a list of markets
from the config in one of two modes:

- `make` rests quotes on the thin venue around the deep venue's price and hedges every fill on the deep venue at once.
- `cross` takes both books wherever they cross by more than fees.

`analysis/` holds the recorder, the transform and the chart behind the write-up.

## make

Robinhood Chain is deep and tight, 2 to 15 bps of spread. The mainnet pre-IPO markets are thin and wide, 60 to 250 bps.
The bot keeps a moving average of the thin venue's premium over the deep venue for each market, seeded from the config
or warmed from live data for one time constant, and rests quotes on the thin venue an edge beyond it, never inside its
own touch: on a rich market asks only, above the premium; on a cheap one bids only, below the discount. The average only
ever pushes a quote away from the deep venue's price, never across it, so an entry never buys on the dearer venue or
sells on the cheaper one. Takers crossing the thin venue's spread fill them. Every fill is hedged at once with an IOC on the deep venue, so inventory is always a hedged pair, capped per
market, and the exit rests where the round trip nets the edge against the pair's entry gap. Nothing rests on the deep
venue. A position in a market not on the list is only reduced.

Pick markets where the thin venue's book reaches past the deep venue's price and the deep venue is tight, deep and slow
enough to hedge into. A deep venue that moves faster than a quote can be re-priced picks the quote off.

## cross

On some markets one venue's best bid sits above the other's best ask for minutes at a time. Whenever the books cross by
more than both venues' taker fees plus a minimum edge, the bot walks both books to the size they cross by and takes both
sides at once with two IOCs, within a clip and the inventory cap. The position that leaves is a hedged pair, and the
reverse cross unwinds it the same way. A leg the other venue did not fill in full is squared. Nothing rests.

## Run

```
uv sync
cp config.make.example.yaml config.yaml    # OPENAI and ANTHROPIC around their premium; config.cross.example.yaml takes AI's crosses
export LIGHTER_ACCOUNT_INDEX=... LIGHTER_API_KEY=... RH_ACCOUNT_INDEX=... RH_API_KEY=...
uv run lighter-arb --config config.yaml
```

API keys must allow taker orders: the hedge is an IOC. One process per account: positions, nonces and the cancel-all are
account-wide, and a process reduces any position outside its market list, so running both modes at once needs a separate
sub-account on each venue.

Logs go to stdout, one line per fill, hedge, cross, reject and halt, plus a `status` line once a minute with each
market's gap, premium average, resting orders, liquidation buffer, inventory and unhedged amount. The premium average is
what to seed the market with next time.

Under systemd, set `Restart=no`. A halted bot should stay down until someone has looked. Every resting order carries the
venue's five-minute expiry, so the venue clears the books by itself if the process dies.

Lighter meters transactions by volume quota: creates, modifies and cancel-alls each spend one, a plain cancel is free, and
an account earns one per 2 USD traded plus one free transaction every 15 seconds. The bot lives on the free ones: creates and
modifies go out one per 16 seconds per venue, a resting order is moved only when its target has drifted 10 bps, and a
rate-limit reject cancels what rests and pauses the venue for a minute rather than re-sending.

## Layout

| file | what |
|---|---|
| `strategy.py` | the pure math: the two quotes and the cross walk |
| `venue.py` | one Lighter instance on the SDK: books, positions, fills, signed txs over the socket, cancel-all, reconcile |
| `guard.py` | the liquidation guard: buffer, top-up, shrink |
| `bot.py` | market loops, hedging, risk, CLI |
| `config.py` | YAML with `${ENV}` substitution; the two example configs are the two runs from the write-up |
| `analysis/` | the websocket recorder, the transform to per-second tables, the chart, and the results of the capture |
