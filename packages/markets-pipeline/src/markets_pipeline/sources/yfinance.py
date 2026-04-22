"""yfinance source. Daily OHLC only; no intraday data.

Native IDs are of the form "<ticker>:<channel>" where channel is
open|high|low|close. Four observations per trading day per ticker.
"""
from __future__ import annotations

import logging
from collections.abc import Iterator
from datetime import date, datetime, timezone
from decimal import Decimal
from typing import Any

from markets_core.domain.observation import Observation
from markets_core.domain.series import SeriesId
from markets_core.errors import DataSourceError, SeriesNotFoundError

logger = logging.getLogger(__name__)

_SUPPORTED_CHANNELS = ("open", "high", "low", "close")
_SUPPORTED_TICKERS = ("DX-Y.NYB", "BZ=F")


def _split_native(native_id: str) -> tuple[str, str]:
    if ":" not in native_id:
        raise SeriesNotFoundError(
            f"yfinance native_id must be '<ticker>:<channel>', got {native_id!r}"
        )
    ticker, channel = native_id.rsplit(":", 1)
    if ticker not in _SUPPORTED_TICKERS:
        raise SeriesNotFoundError(f"unsupported yfinance ticker: {ticker}")
    if channel not in _SUPPORTED_CHANNELS:
        raise SeriesNotFoundError(f"unsupported yfinance channel: {channel}")
    return ticker, channel


class YfinanceSource:
    source_name: str = "yfinance"

    def __init__(self, ticker_allowlist: tuple[str, ...] = _SUPPORTED_TICKERS):
        self._allowed = ticker_allowlist

    def fetch(
        self,
        series_id: SeriesId,
        start: date,
        end: date,
        as_of: date | None = None,
    ) -> Iterator[Observation]:
        ticker, channel = _split_native(series_id.native_id)
        if ticker not in self._allowed:
            raise SeriesNotFoundError(f"ticker not in allowlist: {ticker}")
        import yfinance as yf
        try:
            tkr = yf.Ticker(ticker)
            hist = tkr.history(
                start=start.isoformat(),
                end=end.isoformat(),
                interval="1d",
                auto_adjust=False,
            )
        except Exception as e:
            raise DataSourceError(f"yfinance fetch failed for {ticker}: {e}") from e
        if hist is None or hist.empty:
            return
        column_map = {"open": "Open", "high": "High", "low": "Low", "close": "Close"}
        col = column_map[channel]
        as_of_dt = as_of or date.today()
        now = datetime.now(timezone.utc)
        canonical = str(series_id)
        for idx, row in hist.iterrows():
            obs_date = idx.date() if hasattr(idx, "date") else idx
            value: Any = row[col]
            if value is None:
                continue
            yield Observation(
                series_id=canonical,
                observation_date=obs_date,
                as_of_date=as_of_dt,
                value=Decimal(str(round(float(value), 8))),
                ingested_at=now,
                source_revision=None,
            )

    def health_check(self) -> dict:
        try:
            import yfinance as yf
            _ = yf.Ticker(_SUPPORTED_TICKERS[0]).info
            return {"healthy": True}
        except Exception as e:
            return {"healthy": False, "error": str(e)}

    def metadata(self, series_id: SeriesId) -> dict:
        ticker, channel = _split_native(series_id.native_id)
        return {
            "source": self.source_name,
            "native_id": series_id.native_id,
            "ticker": ticker,
            "channel": channel,
            "frequency": "daily",
        }
