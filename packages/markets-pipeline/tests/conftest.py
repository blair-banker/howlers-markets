"""Root test conftest: fixtures shared across unit, contract, and integration tests.

Stage 4 fixtures (``warehouse``, ``stage4_seeded_fixtures``) live here so that
both ``tests/integration/`` and ``tests/contract/`` directories can discover them.
The integration conftest re-uses them by importing from here (or just relies on
pytest fixture inheritance from the parent directory).
"""
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal
import os

import pytest

from markets_core.domain.observation import Observation
from markets_pipeline.stores.timescale import TimescaleWarehouse
from markets_pipeline.transforms.zscore import ZScoreTransform
from markets_pipeline.transforms.yoy_change import YoYChangeTransform
from markets_pipeline.transforms.trend import TrendTransform


SERIES_BY_VAR = {
    "us_10y_treasury":  "fred:DGS10",
    "us_2y_treasury":   "fred:DGS2",
    "us_real_10y":      "fred:DFII10",
    "dxy":              "yfinance:DX-Y.NYB:close",
    "brent_oil":        "yfinance:BZ=F:close",
    "core_cpi_yoy":     "fred:CPILFESL",
    "headline_cpi_yoy": "fred:CPIAUCSL",
}


@pytest.fixture
def warehouse() -> TimescaleWarehouse:
    dsn = os.environ.get("DATABASE_URL") or "postgresql://postgres:your_password_here@localhost:5432/markets"
    return TimescaleWarehouse(dsn=dsn)


@pytest.fixture
def stage4_seeded_fixtures(warehouse):
    """Seed enough synthetic history for every variable so the classifier
    can run at ``good_date`` and will raise InsufficientDataError at
    ``insufficient_date`` (before any data exists).

    Provides daily synthetic values for all 7 series starting ~410 days
    before good_date so that zscore (252-day window), yoy_change (365-day
    lookback), and trend (252-day window) all have sufficient history.

    CPI series receive daily values (not just the 1st of each month) so that
    YoYChangeTransform.compute(good_date) can find a value at the exact
    target date.

    IMPORTANT: does NOT touch ontology.  Assumes ontology seed is already loaded.
    """
    good_date = date(2026, 4, 15)
    insufficient_date = date(2026, 1, 1)  # before any synthetic data
    start = date(2025, 3, 1)  # 410 days before good_date; >365 for yoy lookback

    now = datetime.now(timezone.utc)
    obs: list[Observation] = []
    days = (good_date - start).days + 1
    for var_name, sid in SERIES_BY_VAR.items():
        for i in range(days):
            d = start + timedelta(days=i)
            if var_name == "us_real_10y":
                # Drifts near zero — won't reliably trigger stress regimes.
                value = Decimal(str(round(0.5 + (i - days // 2) * 0.005, 6)))
            elif var_name == "dxy":
                value = Decimal(str(round(100 + i * 0.05, 6)))
            elif var_name == "us_10y_treasury":
                value = Decimal(str(round(4.0 + i * 0.002, 6)))
            elif var_name == "us_2y_treasury":
                value = Decimal("4.2")
            elif var_name == "brent_oil":
                # Mild oscillation around 80 — should not trigger zscore > 2.0
                value = Decimal(str(round(80 + (i % 30) * 0.2, 6)))
            elif var_name in ("core_cpi_yoy", "headline_cpi_yoy"):
                # Daily values so YoYChangeTransform finds good_date in the map.
                # Keep yoy very small to avoid supply_shock triggers.
                value = Decimal(str(round(0.03 + i * 0.00001, 8)))
            else:
                continue
            obs.append(Observation(
                series_id=sid,
                observation_date=d,
                as_of_date=d,
                value=value,
                ingested_at=now,
                source_revision=None,
            ))
    warehouse.upsert_observations(obs)

    # Run transforms so the classifier has features at good_date.
    ZScoreTransform(warehouse).compute(good_date)
    YoYChangeTransform(warehouse).compute(good_date)
    TrendTransform(warehouse).compute(good_date)

    yield {
        "good_date": good_date,
        "insufficient_date": insufficient_date,
        "series_ids": list(SERIES_BY_VAR.values()),
    }

    # Cleanup: remove synthetic observations, derived rows, and classifier output.
    for sid in SERIES_BY_VAR.values():
        warehouse.execute_sql(
            "DELETE FROM observations.observations WHERE series_id = %s", (sid,)
        )
        warehouse.execute_sql(
            "DELETE FROM derived.zscores WHERE series_id = %s", (sid,)
        )
        warehouse.execute_sql(
            "DELETE FROM derived.yoy_changes WHERE series_id = %s", (sid,)
        )
        warehouse.execute_sql(
            "DELETE FROM derived.trends WHERE series_id = %s", (sid,)
        )
    warehouse.execute_sql(
        "DELETE FROM regimes.regime_states WHERE as_of_date = %s",
        (good_date,),
    )
