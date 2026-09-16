# Data and analysis

The capture and the numbers behind the write-up: every order-book snapshot and delta, every print and the market-stats
feed of both venues, for the 43 perpetual markets listed on both, from 2026-09-15 20:58 to 2026-09-16 07:50 UTC (US
markets closed throughout). Times are the recorder's receive time, one clock for both venues, on a box about 150 ms from
Lighter. `sym` is the market symbol; `l_` is Lighter mainnet, `r_` is Robinhood Chain; bps are 1e-4 of price.

```
uv sync --extra analysis
cd analysis
python record.py                                   # runs until stopped: data/raw_{venue}_{YYYYMMDD_HH}.jsonl.gz
ls data/raw_*.jsonl.gz | xargs -P 24 -n 1 python transform.py hour   # one process per hour file
python transform.py merge                          # derived/*.parquet and *.csv, both venues joined
python chart.py                                    # results/touch_cross.png and results/why_make.png
```

## What `record.py` writes

One line per websocket message, `{"t": receive time, "v": venue, "m": the message}`, gzipped, one file per venue per UTC
hour. Keepalive is the venue's own: a client `{"type":"ping"}` every 20 s whether or not the stream is busy (the venue
closes a connection that stays quiet on that for about two minutes).

## What `transform.py` builds

| file | one row per | columns |
|---|---|---|
| `bbo_{lighter,rh}.parquet` | change of the top of book | `t`, `sym`, `bid`, `ask`, `bid_sz`, `ask_sz`. NaN prices mark a reconnect: the book was unknown until its next snapshot. |
| `trades_{lighter,rh}.parquet` | print | `t`, `sym`, `price`, `size`, `taker` (side of the aggressor), `liquidation`, `trade_id`, `venue_ts`. |
| `stats_{lighter,rh}.parquet` | market-stats update | `t`, `sym`, `index`, `mark`, `last`, `funding`, `volume_usd` (rolling 24 h). |
| `grid_{lighter,rh}.parquet` | market and second | `bid`, `ask`, `buy100/500/2000` (price reached buying that many USD through the asks), `sell100/500/2000` (through the bids); NaN where the book held less. |
| `pair_1s.parquet` | market and second, both venues | the two grids side by side plus `gap_bps` (Lighter mid over RH mid), both spreads, `cross_touch_bps` (positive when one venue's bid is over the other's ask) and `cross100/500/2000_bps` (the edge of selling that clip into one venue's bids and buying it from the other's asks, both walked through the book). |
| `cross_episodes_touch.csv` | run of the touches being crossed, event resolution | `sym`, `start`, `end`, `duration_s`, `direction` (+1 Lighter bid over RH ask), `max_bps`, `mean_bps`. |
| `cross_episodes_sized.csv` | run of consecutive seconds with an executable cross at a clip | `clip_usd`, `sym`, `start`, `end`, `duration_s`, `max_bps`, `mean_bps`. |
| `summary_by_market.csv` | market | coverage, gap percentiles, spreads, share of time crossed and executable, run counts and lengths, index and mark gaps (a gap that is also in the index is a reference difference), mean funding, 24 h volumes. |
| `coverage.csv` | venue and hour | top-of-book rows and reconnects. |

Each hour is seeded with the book state replayed from the previous hour's file, so a run that spans an hour boundary is
not cut. A run interrupted by a reconnect hole of under five seconds is bridged when the cross is still there after it.

## `results/`

The tables and the two charts from the 15-16 September capture: the touch cross for five markets, and why the maker rests on Lighter and hedges on Robinhood Chain (the cost of taking size on each venue, and the ANTHROPIC premium around its 15-minute average). Caveats that belong in any use of them: until 06:45
UTC the recorder reconnected every two minutes (the keepalive bug above), so there is a one-to-two second hole every
two minutes before then; books are re-snapshotted after each so they stay consistent, prints inside those seconds are
missing. Two hourly files were written by two recorder processes in turn and are read member by member. The last hour
ends at 07:50 UTC when the recorder was stopped. Executable edges are gross of fees.
