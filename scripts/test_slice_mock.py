#!/usr/bin/env python3
"""Test vertical slice with mock FRED data."""

import os
import sys
from datetime import date, datetime, timedelta
from pathlib import Path
from decimal import Decimal

# Add to path
sys.path.insert(0, str(Path(__file__).parent / "packages" / "markets-core" / "src"))
sys.path.insert(0, str(Path(__file__).parent / "packages" / "markets-pipeline" / "src"))

from markets_pipeline.stores.parquet import ParquetRawStore
from markets_pipeline.stores.timescale import TimescaleWarehouse
from markets_pipeline.transforms.zscore import ZScoreTransform
from markets_pipeline.classifiers.rule_based import RuleBasedClassifier
from markets_core.domain.series import SeriesId
from markets_core.domain.observation import Observation


def main():
    """Run test with mock data."""
    db_dsn = os.environ.get("DATABASE_URL") or "postgresql://postgres:your_password_here@localhost:5432/markets"
    
    print("=== Vertical Slice Test (Mock Data) ===")
    
    # Create mock observations
    print("\n[Stage 1] Creating mock observations...")
    try:
        # Create 90 days of synthetic treasury yield data
        observations = []
        end = date.today()
        for i in range(90):
            obs_date = end - timedelta(days=90-i)
            # Realistic yield: 4.0% to 5.0%
            value = 4.0 + (i % 30) * 0.02
            
            observations.append(Observation(
                series_id="fred:DGS10",
                observation_date=obs_date,
                as_of_date=end,
                value=Decimal(str(value)),
                ingested_at=datetime.now(),
                source_revision=None,
            ))
        
        print(f"  Created {len(observations)} mock observations")
    except Exception as e:
        print(f"  ERROR: {e}")
        return False
    
    # Stage 2: Normalize
    print("\n[Stage 2] Normalizing into TimescaleDB...")
    try:
        warehouse = TimescaleWarehouse(dsn=db_dsn)
        warehouse.upsert_observations(observations)
        print(f"  Upserted {len(observations)} observations")
    except Exception as e:
        print(f"  ERROR: {e}")
        import traceback
        traceback.print_exc()
        return False
    
    # Stage 3: Derive
    print("\n[Stage 3] Computing z-scores...")
    try:
        transform = ZScoreTransform(warehouse=warehouse, window_days=63)
        zscores = transform.compute(series_id="fred:DGS10", as_of=end)
        print(f"  Computed {len(zscores)} z-scores")
        
        if zscores:
            warehouse.upsert_zscores(zscores)
            print(f"  Upserted z-scores")
    except Exception as e:
        print(f"  ERROR: {e}")
        import traceback
        traceback.print_exc()
        return False
    
    # Stage 4: Classify
    print("\n[Stage 4] Classifying regimes...")
    try:
        classifier = RuleBasedClassifier(warehouse=warehouse)
        regimes = classifier.classify(as_of=end)
        print(f"  Classified {len(regimes)} regime(s)")
        
        for regime in regimes:
            print(f"    - {regime['regime_name']}: confidence={regime['confidence']}")
        
        if regimes:
            warehouse.upsert_regime_states(regimes)
            print(f"  Upserted regime states")
    except Exception as e:
        print(f"  ERROR: {e}")
        import traceback
        traceback.print_exc()
        return False
    
    print("\n✓ Vertical slice test completed successfully!")
    return True


if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)