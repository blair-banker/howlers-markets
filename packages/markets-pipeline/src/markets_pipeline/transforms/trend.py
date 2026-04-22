"""Trend transform: OLS slope of values vs. day index over a trailing window."""
from __future__ import annotations
from datetime import date, timedelta
from decimal import Decimal

from markets_pipeline.transforms.zscore import _forward_fill_to_daily


DEFAULT_WINDOWS = (63, 252)


def compute_trend_slope(
    daily: list[tuple[date, Decimal]], window_days: int
) -> Decimal | None:
    """Return OLS slope of value vs. day index over the trailing window.

    `daily` must be sorted ascending and forward-filled.
    Returns None if fewer than window_days rows.
    """
    if len(daily) < window_days:
        return None
    window = daily[-window_days:]
    ys = [float(v) for _, v in window]
    n = len(ys)
    xs = list(range(n))
    x_mean = sum(xs) / n
    y_mean = sum(ys) / n
    num = sum((xs[i] - x_mean) * (ys[i] - y_mean) for i in range(n))
    den = sum((xs[i] - x_mean) ** 2 for i in range(n))
    if den == 0:
        return Decimal("0")
    slope = num / den
    return Decimal(str(round(slope, 6)))


class TrendTransform:
    def __init__(self, warehouse, windows: tuple[int, ...] = DEFAULT_WINDOWS):
        self.warehouse = warehouse
        self.windows = windows

    def compute(self, as_of_date: date) -> None:
        variables = self.warehouse.read_ontology_variables()
        series_ids = [v["primary_series"] for v in variables if v.get("primary_series")]
        if not series_ids:
            return
        start = as_of_date - timedelta(days=400)
        rows = self.warehouse.read_observations_point_in_time(
            series_ids=series_ids, as_of_date=as_of_date, start=start
        )
        grouped: dict[str, list[dict]] = {sid: [] for sid in series_ids}
        for r in rows:
            grouped[r["series_id"]].append(
                {"observation_date": r["observation_date"], "value": r["value"]}
            )
        out_rows: list[dict] = []
        for sid, obs in grouped.items():
            daily = _forward_fill_to_daily(obs, as_of_date)
            for w in self.windows:
                slope = compute_trend_slope(daily, w)
                if slope is None:
                    continue
                out_rows.append({
                    "series_id": sid,
                    "observation_date": as_of_date,
                    "as_of_date": as_of_date,
                    "window_days": w,
                    "slope": slope,
                })
        self.warehouse.upsert_trends(out_rows)
