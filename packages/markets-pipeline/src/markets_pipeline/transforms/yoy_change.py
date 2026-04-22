"""Year-over-year percent change transform."""
from __future__ import annotations
from datetime import date, timedelta
from decimal import Decimal


def compute_yoy_pct(
    values: dict[date, Decimal], target: date, max_offset_days: int = 90
) -> Decimal | None:
    """Return (value[target] - value[target-365d]) / value[target-365d].

    ``values`` may be sparse (e.g. monthly CPI).  If the exact target date
    has no observation, search backward up to ``max_offset_days`` for the
    most recent prior value (forward-fill semantics).

    If the exact 365-day-prior date is absent, search backward up to
    max_offset_days for the nearest prior observation.
    Returns None if target value or any eligible prior value is missing.
    """
    # Forward-fill to target: find the most recent observation on or before target
    current = None
    for offset in range(0, max_offset_days + 1):
        d = target - timedelta(days=offset)
        if d in values and values[d] is not None:
            current = values[d]
            break
    if current is None:
        return None
    ideal = target - timedelta(days=365)
    for offset in range(0, max_offset_days + 1):
        d = ideal - timedelta(days=offset)
        prior = values.get(d)
        if prior is not None and prior != Decimal(0):
            return (current - prior) / prior
    return None


class YoYChangeTransform:
    def __init__(self, warehouse):
        self.warehouse = warehouse

    def compute(self, as_of_date: date) -> None:
        variables = self.warehouse.read_ontology_variables()
        series_ids = [v["primary_series"] for v in variables if v.get("primary_series")]
        if not series_ids:
            return
        start = as_of_date - timedelta(days=400)
        rows = self.warehouse.read_observations_point_in_time(
            series_ids=series_ids, as_of_date=as_of_date, start=start
        )
        per_series: dict[str, dict[date, Decimal]] = {sid: {} for sid in series_ids}
        for r in rows:
            if r["value"] is not None:
                per_series[r["series_id"]][r["observation_date"]] = Decimal(r["value"])
        out_rows: list[dict] = []
        for sid, m in per_series.items():
            y = compute_yoy_pct(m, as_of_date)
            if y is None:
                continue
            out_rows.append({
                "series_id": sid,
                "observation_date": as_of_date,
                "as_of_date": as_of_date,
                "yoy_pct": y.quantize(Decimal("0.000001")),
            })
        self.warehouse.upsert_yoy_changes(out_rows)
