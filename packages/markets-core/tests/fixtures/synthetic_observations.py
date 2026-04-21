from datetime import datetime
from decimal import Decimal
from markets_core.domain.observation import Observation
from markets_core.domain.series import SeriesId


def synthetic_observations():
    """Unrealistic synthetic observations for testing."""
    return [
        Observation(
            series_id="test:FAKE_RATE",
            observation_date="2026-04-01",
            as_of_date="2026-04-01",
            value=Decimal("1.23"),
            ingested_at=datetime(2026, 4, 1),
        ),
        Observation(
            series_id="test:FAKE_RATE",
            observation_date="2026-04-02",
            as_of_date="2026-04-02",
            value=Decimal("1.24"),
            ingested_at=datetime(2026, 4, 2),
        ),
    ]