import pytest
from datetime import datetime, date
from decimal import Decimal
from markets_core.domain.observation import Observation


def test_observation_creation():
    obs = Observation(
        series_id="test:FAKE",
        observation_date=date(2026, 4, 1),
        as_of_date=date(2026, 4, 1),
        value=Decimal("1.0"),
        ingested_at=datetime(2026, 4, 1),
    )
    assert obs.value == Decimal("1.0")


def test_observation_invalid_dates():
    with pytest.raises(ValueError):
        Observation(
            series_id="test:FAKE",
            observation_date=date(2026, 4, 2),
            as_of_date=date(2026, 4, 1),  # before observation
            value=Decimal("1.0"),
            ingested_at=datetime(2026, 4, 1),
        )