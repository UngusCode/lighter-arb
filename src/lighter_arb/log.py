"""Plain-text logging: one line per event, `key=value` fields, standard library only."""

from __future__ import annotations

import logging


class Log:
    def __init__(self, name: str) -> None:
        self._l = logging.getLogger(name)

    def _emit(self, level: int, event: str, kw: dict) -> None:
        self._l.log(level, "%s %s", event, " ".join(f"{k}={v}" for k, v in kw.items()))

    def info(self, event: str, **kw: object) -> None:
        self._emit(logging.INFO, event, kw)

    def warning(self, event: str, **kw: object) -> None:
        self._emit(logging.WARNING, event, kw)

    def error(self, event: str, **kw: object) -> None:
        self._emit(logging.ERROR, event, kw)


def setup() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s", datefmt="%Y-%m-%d %H:%M:%S")
