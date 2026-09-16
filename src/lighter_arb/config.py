"""YAML config with ${ENV} / ${ENV:-default} substitution (comment lines are left alone)."""

from __future__ import annotations

import os
import re
from pathlib import Path
from typing import Literal

import yaml
from pydantic import BaseModel, model_validator

_ENV = re.compile(r"\$\{([A-Z0-9_]+)(?::-([^}]*))?\}")


class VenueCfg(BaseModel):
    base_url: str
    account_index: int
    api_key_index: int
    api_key_private_key: str
    taker_fee_bps: float = 0.0  # what this account pays to take, from the venue's fee schedule; cross mode nets it off the edge


class StrategyCfg(BaseModel):
    mode: Literal["make", "cross"] = "make"  # make: rest on the quote venue, hedge fills on the other. cross: take both books where they cross
    edge_bps: float = 20.0  # make: an entry rests at least this far past the premium average; a round trip nets at least this
    premium_tau_s: float = 900.0  # make: time constant of each market's average of the quote venue's premium over the hedge venue, in bps.
    # Seeded from `markets` it is live at once; unseeded, entries wait until it has run for one time constant
    cross_min_edge_bps: float = 10.0  # cross: take only where the books cross by at least this after both venues' taker fees
    clip_usd: float = 100.0  # per order
    max_inventory_usd: float = 500.0  # per market on the quote venue; an entry rests only if a clip fits under it


class Config(BaseModel):
    venues: dict[str, VenueCfg]  # exactly `quote` (the thin venue we rest on) and `hedge` (the deep venue we take on)
    markets: dict[str, float | None]  # symbol -> premium seed in bps (see strategy.premium_tau_s), or null to warm it from live data
    leverage: int = 3  # every market is set to cross margin at this leverage when it enters; isolated where the venue insists
    strategy: StrategyCfg = StrategyCfg()
    drawdown_usd: float = 50.0  # halt when venue-reported equity since start falls below minus this
    liq_buffer: float = 0.15  # liquidation guard: top up an isolated leg under this much adverse room; shrink the pair under half of it

    @model_validator(mode="after")
    def _check(self) -> Config:
        if set(self.venues) != {"quote", "hedge"}:
            raise ValueError("venues must be exactly `quote` and `hedge`")
        if not self.markets or len(set(self.markets)) != len(self.markets):
            raise ValueError("markets must list at least one symbol, each once")
        return self


def _env(m: re.Match[str]) -> str:
    return os.environ.get(m.group(1), m.group(2) or "")


def load(path: str) -> Config:
    text = "\n".join(ln if ln.lstrip().startswith("#") else _ENV.sub(_env, ln) for ln in Path(path).read_text().splitlines())
    return Config.model_validate(yaml.safe_load(text))
