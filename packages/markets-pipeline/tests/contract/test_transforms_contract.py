"""Contract tests for the three Stage 4 Transform implementations.

These are integration tests because Transform.compute has DB side effects.
The contract: .compute(d) is idempotent for a given as-of date and writes
at least one row per series it processes.
"""
from datetime import date, timedelta
from decimal import Decimal
import os
import pytest

from markets_pipeline.stores.timescale import TimescaleWarehouse
from markets_pipeline.transforms.zscore import ZScoreTransform
from markets_pipeline.transforms.yoy_change import YoYChangeTransform
from markets_pipeline.transforms.trend import TrendTransform


pytestmark = pytest.mark.integration


@pytest.fixture
def warehouse() -> TimescaleWarehouse:
    dsn = os.environ.get("DATABASE_URL") or "postgresql://postgres:your_password_here@localhost:5432/markets"
    return TimescaleWarehouse(dsn=dsn)


@pytest.fixture
def synthetic_series(warehouse):
    """Insert 320 daily observations for a synthetic test series; add an ontology row."""
    from markets_core.domain.observation import Observation
    from datetime import datetime, timezone
    sid_str = "test:TRANSFORM_CONTRACT"
    warehouse.execute_sql(
        "INSERT INTO ontology.variables (name, display_name, tier, primary_series) "
        "VALUES (%s, %s, %s, %s) ON CONFLICT (name) DO NOTHING",
        ("test_transform_contract", "Test Transform Contract", 2, sid_str),
    )
    start = date(2025, 1, 1)
    now = datetime.now(timezone.utc)
    obs = [
        Observation(series_id=sid_str, observation_date=start + timedelta(days=i),
                    as_of_date=start + timedelta(days=i),
                    value=Decimal(str(1.0 + i * 0.01)),
                    ingested_at=now, source_revision=None)
        for i in range(480)
    ]
    warehouse.upsert_observations(obs)
    # as_of = 2026-04-26: one-year-prior = 2025-04-26 (day 115), well within the dataset
    yield sid_str, start + timedelta(days=480 - 1)
    warehouse.execute_sql("DELETE FROM observations.observations WHERE series_id = %s", (sid_str,))
    warehouse.execute_sql("DELETE FROM derived.zscores WHERE series_id = %s", (sid_str,))
    warehouse.execute_sql("DELETE FROM derived.yoy_changes WHERE series_id = %s", (sid_str,))
    warehouse.execute_sql("DELETE FROM derived.trends WHERE series_id = %s", (sid_str,))
    warehouse.execute_sql("DELETE FROM ontology.variables WHERE name = %s",
                          ("test_transform_contract",))


class TestZScoreTransformContract:
    def test_compute_writes_row(self, warehouse, synthetic_series):
        sid, as_of = synthetic_series
        ZScoreTransform(warehouse).compute(as_of)
        v = warehouse.read_zscore_point_in_time(sid, window_days=63, as_of_date=as_of)
        assert v is not None

    def test_compute_is_idempotent(self, warehouse, synthetic_series):
        sid, as_of = synthetic_series
        t = ZScoreTransform(warehouse)
        t.compute(as_of)
        v1 = warehouse.read_zscore_point_in_time(sid, 63, as_of)
        t.compute(as_of)
        v2 = warehouse.read_zscore_point_in_time(sid, 63, as_of)
        assert v1 == v2


class TestYoYChangeTransformContract:
    def test_compute_writes_row(self, warehouse, synthetic_series):
        sid, as_of = synthetic_series
        YoYChangeTransform(warehouse).compute(as_of)
        v = warehouse.read_yoy_change_point_in_time(sid, as_of)
        assert v is not None

    def test_compute_is_idempotent(self, warehouse, synthetic_series):
        sid, as_of = synthetic_series
        t = YoYChangeTransform(warehouse)
        t.compute(as_of)
        v1 = warehouse.read_yoy_change_point_in_time(sid, as_of)
        t.compute(as_of)
        v2 = warehouse.read_yoy_change_point_in_time(sid, as_of)
        assert v1 == v2


class TestTrendTransformContract:
    def test_compute_writes_row(self, warehouse, synthetic_series):
        sid, as_of = synthetic_series
        TrendTransform(warehouse).compute(as_of)
        v = warehouse.read_trend_point_in_time(sid, window_days=63, as_of_date=as_of)
        assert v is not None

    def test_compute_is_idempotent(self, warehouse, synthetic_series):
        sid, as_of = synthetic_series
        t = TrendTransform(warehouse)
        t.compute(as_of)
        v1 = warehouse.read_trend_point_in_time(sid, 63, as_of)
        t.compute(as_of)
        v2 = warehouse.read_trend_point_in_time(sid, 63, as_of)
        assert v1 == v2
