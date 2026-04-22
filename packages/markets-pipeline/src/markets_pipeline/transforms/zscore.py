"""Z-score transform: computes rolling z-score of a series' value.

At each run we compute z-score for observation_date = as_of_date only, using
the point-in-time history of the series (latest known vintage per observation
date, all with as_of_date <= as_of_date).
"""
from __future__ import annotations
from datetime import date, timedelta
from decimal import Decimal


DEFAULT_WINDOWS = (63, 252)


def compute_zscore(
    daily: list[tuple[date, Decimal]], window_days: int
) -> Decimal | None:
    """Return z-score of the latest value relative to the trailing window.

    `daily` must be sorted ascending by date and already forward-filled.
    Returns None if fewer than window_days observations or stddev is 0.
    """
    if len(daily) < window_days:
        return None
    window = daily[-window_days:]
    values = [float(v) for _, v in window]
    n = len(values)
    mean = sum(values) / n
    var = sum((x - mean) ** 2 for x in values) / n
    std = var ** 0.5
    if std == 0:
        return None
    latest = float(window[-1][1])
    z = (latest - mean) / std
    return Decimal(str(round(z, 4)))


def _forward_fill_to_daily(
    observations: list[dict], end: date
) -> list[tuple[date, Decimal]]:
    """Turn sparse observations into a dense daily series ending at `end`.

    observations: list of {"observation_date": date, "value": Decimal|None}
    sorted ascending. Gaps are forward-filled using the last known value.
    Days before the first observation are omitted.
    """
    if not observations:
        return []
    observations = sorted(observations, key=lambda r: r["observation_date"])
    out: list[tuple[date, Decimal]] = []
    obs_iter = iter(observations)
    current = next(obs_iter)
    next_obs = next(obs_iter, None)
    d = current["observation_date"]
    last_value = current["value"]
    while d <= end:
        while next_obs is not None and next_obs["observation_date"] <= d:
            if next_obs["value"] is not None:
                last_value = next_obs["value"]
            current = next_obs
            next_obs = next(obs_iter, None)
        if last_value is not None:
            out.append((d, Decimal(last_value)))
        d = d + timedelta(days=1)
    return out


class ZScoreTransform:
    """Transform implementation. Writes derived.zscores."""

    def __init__(self, warehouse, windows: tuple[int, ...] = DEFAULT_WINDOWS):
        self.warehouse = warehouse
        self.windows = windows

    def compute(self, as_of_date: date) -> None:
        variables = self.warehouse.read_ontology_variables()
        series_ids = [v["primary_series"] for v in variables if v.get("primary_series")]
        if not series_ids:
            return
        grouped = self._read_history(series_ids, as_of_date)
        out_rows: list[dict] = []
        for series_id, obs in grouped.items():
            daily = _forward_fill_to_daily(obs, as_of_date)
            for w in self.windows:
                z = compute_zscore(daily, w)
                if z is None:
                    continue
                out_rows.append({
                    "series_id": series_id,
                    "observation_date": as_of_date,
                    "as_of_date": as_of_date,
                    "window_days": w,
                    "zscore": z,
                })
        self.warehouse.upsert_zscores(out_rows)

    def _read_history(self, series_ids: list[str], as_of_date: date) -> dict[str, list[dict]]:
        start = as_of_date - timedelta(days=400)
        rows = self.warehouse.read_observations_point_in_time(
            series_ids=series_ids, as_of_date=as_of_date, start=start
        )
        grouped: dict[str, list[dict]] = {sid: [] for sid in series_ids}
        for r in rows:
            grouped[r["series_id"]].append(
                {"observation_date": r["observation_date"], "value": r["value"]}
            )
        return grouped
