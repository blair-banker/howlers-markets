import pytest
import os
from datetime import date, datetime, timedelta
from decimal import Decimal
from pathlib import Path
import tempfile

from markets_pipeline.sources.fred import FredSource
from markets_pipeline.stores.parquet import ParquetRawStore
from markets_pipeline.stores.timescale import TimescaleWarehouse
from markets_pipeline.transforms.zscore import ZScoreTransform
from markets_pipeline.classifiers.rule_based import RuleBasedClassifier
from markets_core.domain.series import SeriesId
from markets_core.domain.observation import Observation


@pytest.mark.integration
@pytest.mark.requires_fred_key
def test_fred_source_integration():
    """Test FRED source with real API."""
    fred_key = os.environ.get("FRED_API_KEY")
    if not fred_key:
        pytest.skip("FRED_API_KEY not set")
    
    source = FredSource(api_key=fred_key)
    
    # Test DGS10
    series_id = SeriesId(source="fred", native_id="DGS10")
    
    observations = list(source.fetch(
        series_id=series_id,
        start=date(2026, 3, 1),
        end=date(2026, 3, 15),
    ))
    
    assert len(observations) > 0
    assert observations[0].value is not None


@pytest.mark.integration
def test_parquet_roundtrip():
    """Test Parquet store write/read roundtrip."""
    with tempfile.TemporaryDirectory() as tmpdir:
        store = ParquetRawStore(path=tmpdir)
        
        # Create mock observations
        observations = [
            Observation(
                series_id="fred:DGS10",
                observation_date=date(2026, 3, 1),
                as_of_date=date(2026, 3, 15),
                value=Decimal("4.50"),
                ingested_at=datetime.now(),
                source_revision=None,
            )
        ]
        
        # Write
        series_id = SeriesId(source="fred", native_id="DGS10")
        store.append(
            source="fred",
            series_id=series_id,
            observations=observations,
            as_of=date(2026, 3, 15),
        )
        
        # Verify file exists
        file_path = Path(tmpdir) / "fred" / "DGS10" / "2026-03-15.parquet"
        assert file_path.exists()
        
        # Read back
        read_obs = list(store.read(source="fred", series_id=series_id))
        assert len(read_obs) > 0
        assert read_obs[0].value == Decimal("4.50")