"""Two charts from derived/. Run after transform.py merge.

results/touch_cross.png: the touch cross second by second for five markets. Above the line Lighter's bid is over
Robinhood Chain's ask; below it Robinhood Chain's bid is over Lighter's ask; on the line nothing crosses.
results/why_make.png: why the maker rests an ask on Lighter and hedges on Robinhood Chain. Top: the 15-minute average of
ANTHROPIC's premium over the night, the band above it where the ask rests, and the round trips the rule would have made on
the recorded prints. Bottom: the median cost of taking 500 and 2,000 USD on each venue, from the books walked every second."""

from __future__ import annotations

from pathlib import Path

import matplotlib
import matplotlib.dates
import matplotlib.pyplot as plt
import matplotlib.ticker
import numpy as np
import pandas as pd

matplotlib.use("Agg")
ROOT = Path(__file__).resolve().parent
OUT = ROOT / "results"
OUT.mkdir(parents=True, exist_ok=True)

SURFACE, INK, INK2, MUTED, GRID, AXIS = "#fcfcfb", "#0b0b0b", "#52514e", "#898781", "#e1e0d9", "#c3c2b7"
BLUE, RED, ORANGE = "#2a78d6", "#e34948", "#eb6834"
CLIPS = (100, 500, 2000)
EDGE_BPS = 15.0
TAU_S = 900.0
SYMS = ["OPENAI", "ANTHROPIC", "QQQ", "AI", "BTC"]
NOTES = {
    "OPENAI": "crossed all night, always the same way, 20 to 250 bps",
    "ANTHROPIC": "crossed all night, always the same way, 10 to 100 bps",
    "QQQ": "crossed for the whole measurement, always the same way, a flat 16 bps (same index on both venues)",
    "AI": "crossed in bursts of minutes, in both directions: capturable, then gone",
}

plt.rcParams.update({
    "font.family": "sans-serif",
    "font.size": 11,
    "text.color": INK,
    "axes.labelcolor": INK2,
    "axes.edgecolor": AXIS,
    "axes.linewidth": 0.8,
    "axes.facecolor": SURFACE,
    "figure.facecolor": SURFACE,
    "axes.grid": True,
    "grid.color": GRID,
    "grid.linewidth": 0.8,
    "grid.linestyle": "-",
    "axes.axisbelow": True,
    "xtick.color": MUTED,
    "ytick.color": MUTED,
    "xtick.labelcolor": INK2,
    "ytick.labelcolor": INK2,
    "xtick.labelsize": 10,
    "ytick.labelsize": 10,
    "axes.spines.top": False,
    "axes.spines.right": False,
    "legend.frameon": False,
    "savefig.dpi": 220,
})


def ema(x: np.ndarray, t: np.ndarray, tau: float) -> np.ndarray:
    out = np.empty_like(x)
    out[0] = x[0]
    for i in range(1, len(x)):
        out[i] = out[i - 1] + (1 - np.exp(-(t[i] - t[i - 1]) / tau)) * (x[i] - out[i - 1])
    return out


def round_trips(pair: pd.DataFrame, trades: pd.DataFrame, sym: str) -> tuple[pd.DataFrame, np.ndarray, pd.DataFrame]:
    """The maker rule on the recorded prints: an ask rests above the premium average by the edge (never inside Lighter's own
    ask) and is filled by a buy printing at or above it; the exit rests 2 x edge under the entry gap (never inside Lighter's
    bid) and is filled by a sell printing at or below it. Optimistic on queue position, so the count is a ceiling."""
    g = pair[pair.sym == sym].sort_values("sec").reset_index(drop=True)
    avg = ema(g.gap_bps.values, g.sec.values.astype(float), TAU_S)
    g["avg"] = avg
    tr = trades[trades.sym == sym].sort_values("t").copy()
    tr["sec"] = tr.t.astype(int)
    tr = tr.join(g.set_index("sec")[["avg", "l_ask", "l_bid", "r_ask", "r_bid"]], on="sec").dropna()
    tr["ask"] = np.maximum(tr.r_ask * (1 + (np.maximum(tr.avg, 0) + EDGE_BPS) / 1e4), tr.l_ask)
    rows, open_entry = [], None
    for r in tr.itertuples():
        if open_entry is None:
            if r.taker == "buy" and r.price >= r.ask:
                open_entry = (r.t, (r.price / r.r_ask - 1) * 1e4)
        else:
            t0, gap0 = open_entry
            bid = min(r.r_bid * (1 + (gap0 - 2 * EDGE_BPS) / 1e4), r.l_bid)
            if r.taker == "sell" and r.price <= bid:
                rows.append((t0, gap0, r.t, (r.price / r.r_bid - 1) * 1e4))
                open_entry = None
    return g, avg, pd.DataFrame(rows, columns=["entry_t", "entry_gap", "exit_t", "exit_gap"])


def why_make() -> None:
    sym = "ANTHROPIC"
    pair = pd.read_parquet(ROOT / "derived" / "pair_1s.parquet", columns=["sec", "sym", "gap_bps", "l_ask", "l_bid", "r_ask", "r_bid"], filters=[("sym", "in", [sym, "OPENAI"])])
    trades = pd.read_parquet(ROOT / "derived" / "trades_lighter.parquet", columns=["t", "sym", "price", "size", "taker"], filters=[("sym", "==", sym)])
    g, avg, trips = round_trips(pair, trades, sym)
    t = pd.to_datetime(g.sec, unit="s")
    fig = plt.figure(figsize=(12, 9.4))
    gs = fig.add_gridspec(2, 1, height_ratios=[2.2, 1], hspace=0.38, left=0.15, right=0.97, top=0.97, bottom=0.09)
    ax = fig.add_subplot(gs[0])
    ask_line = np.maximum(avg, 0) + EDGE_BPS
    ax.fill_between(t, avg, ask_line, color=BLUE, alpha=0.12, linewidth=0)
    ax.plot(t, avg, color=BLUE, lw=1.8, label=f"{sym} premium, 15-minute average: Lighter mid over Robinhood Chain mid, not arbitrageable")
    ax.plot(t, ask_line, color=INK2, lw=1.0, label=f"where the ask rests: average + {EDGE_BPS:.0f} bps, never inside Lighter's own ask")
    for r in trips.itertuples():
        te, tx = pd.to_datetime(r.entry_t, unit="s"), pd.to_datetime(r.exit_t, unit="s")
        ax.scatter([te], [r.entry_gap], marker="v", s=80, color=INK, edgecolor=SURFACE, linewidth=1.2, zorder=5)
        ax.scatter([tx], [r.exit_gap], marker="^", s=80, color=SURFACE, edgecolor=INK, linewidth=1.4, zorder=5)
    ax.scatter([], [], marker="v", s=70, color=INK, label="sold on Lighter, bought on Robinhood Chain")
    ax.scatter([], [], marker="^", s=70, color=SURFACE, edgecolor=INK, linewidth=1.4, label=f"bought back {2 * EDGE_BPS:.0f} bps lower, sold on Robinhood Chain")
    net = ((trips.entry_gap - trips.exit_gap)).median() if len(trips) else float("nan")
    ax.set_ylabel("bps")
    ax.set_ylim(-10, max(float(ask_line.max()), float(trips.entry_gap.max())) + 35)  # room for the legend above the data
    ax.xaxis.set_major_formatter(matplotlib.dates.DateFormatter("%H:%M"))
    ax.set_xlabel("UTC, 15 to 16 September 2026")
    ax.text(0.012, 0.03, f"The rule on the recorded prints: {len(trips)} round trips in 11 hours, median hold "
            f"{((trips.exit_t - trips.entry_t) / 60).median():.0f} min,\nmedian {net:.0f} bps between the legs. A ceiling: it assumes first in the queue.",
            transform=ax.transAxes, fontsize=10, color=INK2, va="bottom", ha="left", linespacing=1.5, zorder=6)
    ax.grid(axis="x", visible=False)
    ax.legend(loc="upper right", fontsize=9, ncol=1)
    ax2 = fig.add_subplot(gs[1])
    rows = [("ANTHROPIC", 500), ("ANTHROPIC", 2000), ("OPENAI", 500), ("OPENAI", 2000)]
    grids = {v: pd.read_parquet(ROOT / "derived" / f"grid_{v}.parquet", filters=[("sym", "in", ["ANTHROPIC", "OPENAI"])]) for v in ("lighter", "rh")}
    y = np.arange(len(rows))
    for k, (venue, color, label) in enumerate((("lighter", BLUE, "Lighter"), ("rh", ORANGE, "Robinhood Chain"))):
        vals = []
        for s_, c in rows:
            gg = grids[venue][grids[venue].sym == s_]
            mid = (gg.bid + gg.ask) / 2
            vals.append(float((((gg[f"buy{c}"] / mid - 1) + (1 - gg[f"sell{c}"] / mid)) / 2 * 1e4).median()))
        ax2.barh(y + (0.19 if k == 0 else -0.19), vals, height=0.34, color=color, label=label, zorder=3)
        for yy, v in zip(y + (0.19 if k == 0 else -0.19), vals):
            ax2.text(v + 1.5, yy, f"{v:.0f}" if v >= 10 else f"{v:.1f}", va="center", fontsize=9, color=INK2)
    ax2.set_yticks(y, [f"{s_}, {c:,} USD" for s_, c in rows])
    ax2.invert_yaxis()
    ax2.set_xlim(0, 150)
    ax2.set_xlabel("Median cost to take that size, bps from mid\nA taker on Lighter pays it, a maker there earns it, and the hedge on Robinhood Chain costs a few bps", fontsize=10, linespacing=1.5)
    ax2.grid(axis="y", visible=False)
    ax2.legend(loc="upper right", fontsize=9)
    fig.savefig(OUT / "why_make.png")
    plt.close(fig)
    print("wrote", OUT / "why_make.png")


def main() -> None:
    pair = pd.read_parquet(ROOT / "derived" / "pair_1s.parquet", columns=["sec", "sym", "l_bid", "l_ask", "r_bid", "r_ask"], filters=[("sym", "in", SYMS)])
    summ = pd.read_csv(ROOT / "derived" / "summary_by_market.csv").set_index("sym")
    fig, axes = plt.subplots(len(SYMS), 1, figsize=(12, 13), sharex=True)
    fig.subplots_adjust(left=0.07, right=0.97, top=0.95, bottom=0.06, hspace=0.34)
    for ax, sym in zip(axes, SYMS, strict=True):
        g = pair[pair.sym == sym].sort_values("sec")
        t = pd.to_datetime(g.sec, unit="s")
        up = ((g.l_bid / g.r_ask - 1) * 1e4).clip(lower=0)
        down = ((g.r_bid / g.l_ask - 1) * 1e4).clip(lower=0)
        ax.fill_between(t, 0, up, color=BLUE, alpha=0.16, linewidth=0)
        ax.plot(t, up, color=BLUE, lw=0.7, label="Lighter bid over Robinhood Chain ask")
        ax.fill_between(t, 0, -down, color=RED, alpha=0.16, linewidth=0)
        ax.plot(t, -down, color=RED, lw=0.7, label="Robinhood Chain bid over Lighter ask")
        ax.axhline(0, color=INK2, lw=0.8)
        hi, lo = float(up.quantile(0.999)), float(down.quantile(0.999))
        top = max(6.0, hi * 1.25)
        bot = max(0.15 * top, lo * 1.25) if lo > 0 else 0.15 * top
        ax.set_ylim(-bot, top)
        ax.yaxis.set_major_locator(matplotlib.ticker.MaxNLocator(nbins=4, steps=[1, 2, 5, 10], min_n_ticks=3))
        ax.grid(axis="x", visible=False)
        ax.tick_params(axis="y", labelsize=9)
        ax.text(0.006, 0.94, sym, transform=ax.transAxes, fontsize=12, fontweight="bold", color=INK, va="top")
        if sym in NOTES:
            r = summ.loc[sym]
            run = r.longest_touch_episode_s
            run_s = f"{run / 3600:.1f} h" if run >= 3600 else f"{run / 60:.0f} min" if run >= 60 else f"{run:.0f} s"
            ax.text(0.994, 0.94, f"{NOTES[sym]}  ·  crossed {r.pct_touch_crossed:.0f}% of the time, longest run {run_s}", transform=ax.transAxes, fontsize=9.5, color=INK2, va="top", ha="right")
    axes[2].set_ylabel("Cross at the touch, bps (0 = not crossed)")
    axes[-1].xaxis.set_major_formatter(matplotlib.dates.DateFormatter("%H:%M"))
    axes[-1].set_xlabel("UTC, 15 to 16 September 2026 (US markets closed throughout)")
    handles, labels = axes[SYMS.index("AI")].get_legend_handles_labels()  # the one panel that crosses both ways
    fig.legend(handles, labels, loc="upper left", bbox_to_anchor=(0.035, 0.985), ncol=2, fontsize=10, frameon=False, handlelength=2.2)
    fig.savefig(OUT / "touch_cross.png")
    plt.close(fig)
    print("wrote", OUT / "touch_cross.png")


if __name__ == "__main__":
    main()
    why_make()
