"""Integration tests for warehouse point-in-time reads and derived upserts.

Requires a running Timescale instance with migration 002 applied.
"""
from datetime import date, datetime, timezone
from decimal import Decimal
import os

import pytest

from markets_core.domain.observation import Observation
from markets_pipeline.stores.timescale import TimescaleWarehouse


pytestmark = pytest.mark.integration


@pytest.fixture
def warehouse() -> TimescaleWarehouse:
    dsn = os.environ.get("DATABASE_URL") or "postgresql://postgres:your_password_here@localhost:5432/markets"
    return TimescaleWarehouse(dsn=dsn)


@pytest.fixture
def seeded_observations(warehouse):
    """Insert a few observations for a synthetic series, clean up after."""
    series = "test:INTEGRATION_SERIES"
    now = datetime.now(timezone.utc)
    obs = [
        Observation(series_id=series, observation_date=date(2026, 3, 1),
                    as_of_date=date(2026, 3, 5), value=Decimal("1.00"),
                    ingested_at=now, source_revision=None),
        Observation(series_id=series, observation_date=date(2026, 3, 1),
                    as_of_date=date(2026, 3, 20), value=Decimal("1.10"),
                    ingested_at=now, source_revision="r1"),
    ]
    warehouse.upsert_observations(obs)
    yield series
    warehouse.execute_sql(
        "DELETE FROM observations.observations WHERE series_id = %s", (series,)
    )


def test_point_in_time_read_returns_latest_known_as_of(warehouse, seeded_observations):
    series = seeded_observations
    result = warehouse.read_observations_point_in_time(
        series_ids=[series], as_of_date=date(2026, 3, 10)
    )
    assert len(result) == 1
    assert result[0]["value"] == Decimal("1.00")

    result2 = warehouse.read_observations_point_in_time(
        series_ids=[series], as_of_date=date(2026, 3, 25)
    )
    assert result2[0]["value"] == Decimal("1.10")


def test_read_zscore_returns_none_when_missing(warehouse):
    result = warehouse.read_zscore_point_in_time(
        variable_series_id="test:DEFINITELY_NOT_REAL",
        window_days=63,
        as_of_date=date(2026, 4, 15),
    )
    assert result is None


def test_upsert_and_read_zscore_round_trip(warehouse):
    rows = [{
        "series_id": "test:ROUND_TRIP",
        "observation_date": date(2026, 4, 15),
        "as_of_date": date(2026, 4, 15),
        "window_days": 63,
        "zscore": Decimal("1.2345"),
    }]
    warehouse.upsert_zscores(rows)
    try:
        result = warehouse.read_zscore_point_in_time(
            "test:ROUND_TRIP", window_days=63, as_of_date=date(2026, 4, 15)
        )
        assert result == Decimal("1.2345")
    finally:
        warehouse.execute_sql(
            "DELETE FROM derived.zscores WHERE series_id = %s", ("test:ROUND_TRIP",)
        )


def test_upsert_and_read_yoy_round_trip(warehouse):
    rows = [{
        "series_id": "test:YOY_RT",
        "observation_date": date(2026, 4, 15),
        "as_of_date": date(2026, 4, 15),
        "yoy_pct": Decimal("0.035000"),
    }]
    warehouse.upsert_yoy_changes(rows)
    try:
        result = warehouse.read_yoy_change_point_in_time(
            "test:YOY_RT", as_of_date=date(2026, 4, 15)
        )
        assert result == Decimal("0.035000")
    finally:
        warehouse.execute_sql(
            "DELETE FROM derived.yoy_changes WHERE series_id = %s", ("test:YOY_RT",)
        )


def test_upsert_and_read_trend_round_trip(warehouse):
    rows = [{
        "series_id": "test:TREND_RT",
        "observation_date": date(2026, 4, 15),
        "as_of_date": date(2026, 4, 15),
        "window_days": 63,
        "slope": Decimal("0.002500"),
    }]
    warehouse.upsert_trends(rows)
    try:
        result = warehouse.read_trend_point_in_time(
            "test:TREND_RT", window_days=63, as_of_date=date(2026, 4, 15)
        )
        assert result == Decimal("0.002500")
    finally:
        warehouse.execute_sql(
            "DELETE FROM derived.trends WHERE series_id = %s", ("test:TREND_RT",)
        )


def test_read_raw_value_point_in_time(warehouse, seeded_observations):
    series = seeded_observations
    v = warehouse.read_raw_value_point_in_time(series, as_of_date=date(2026, 3, 25))
    assert v == Decimal("1.10")


def test_read_ontology_version_matches_alembic_head(warehouse):
    v = warehouse.read_ontology_version()
    assert v == "002"


def test_upsert_regime_state_single_row_with_all_columns(warehouse):
    row = {
        "as_of_date": date(2026, 4, 15),
        "classifier_name": "test_classifier",
        "classifier_version": "0.0.0",
        "regime_name": "benign_expansion",
        "confidence": Decimal("0.75"),
        "trigger_variables": ["x", "y"],
        "rationale": "test rationale",
        "rationale_detail": {"winner": "benign_expansion", "triggers": []},
        "ontology_version": "002",
    }
    warehouse.upsert_regime_state(row)
    try:
        import psycopg2
        conn = psycopg2.connect(warehouse.dsn)
        cur = conn.cursor()
        cur.execute(
            "SELECT regime_name, rationale_detail, ontology_version "
            "FROM regimes.regime_states WHERE as_of_date = %s "
            "AND classifier_name = %s AND classifier_version = %s",
            (date(2026, 4, 15), "test_classifier", "0.0.0"),
        )
        got = cur.fetchone()
        cur.close()
        conn.close()
        assert got is not None
        assert got[0] == "benign_expansion"
        assert got[1] == {"winner": "benign_expansion", "triggers": []}
        assert got[2] == "002"
    finally:
        warehouse.execute_sql(
            "DELETE FROM regimes.regime_states WHERE classifier_name = %s",
            ("test_classifier",),
        )
