import pytest
from datetime import date, datetime
from decimal import Decimal

from markets_pipeline.transforms.zscore import ZScoreTransform


class MockWarehouse:
    """Mock warehouse for testing."""
    
    def query_point_in_time(self, series_ids, as_of):
        """Return mock observations."""
        # Return 63 days of observations
        observations = []
        for i in range(63):
            obs_date = date(2026, 1, 1) + __import__('datetime').timedelta(days=i)
            # Simple price series: 4.0 to 5.0 range
            value = 4.0 + (i * 0.01)
            observations.append({
                "observation_date": obs_date,
                "as_of_date": as_of,
                "value": value,
            })
        
        return {
            series_ids[0]: observations
        }


def test_zscore_compute():
    """Test z-score computation."""
    warehouse = MockWarehouse()
    transform = ZScoreTransform(warehouse=warehouse, window_days=63)
    
    zscores = transform.compute(series_id="fred:DGS10", as_of=date(2026, 3, 15))
    
    # Should have some z-scores
    assert len(zscores) > 0
    
    # Check structure
    for z in zscores:
        assert "series_id" in z
        assert "observation_date" in z
        assert "zscore" in z
        assert z["series_id"] == "fred:DGS10"