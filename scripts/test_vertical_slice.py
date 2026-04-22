#!/usr/bin/env python3
"""Manual end-to-end test of the vertical slice."""

import os
import sys
from datetime import date, datetime, timedelta
from pathlib import Path

# Add to path
sys.path.insert(0, str(Path(__file__).parent / "packages" / "markets-core" / "src"))
sys.path.insert(0, str(Path(__file__).parent / "packages" / "markets-pipeline" / "src"))

from markets_pipeline.sources.fred import FredSource
from markets_pipeline.stores.parquet import ParquetRawStore
from markets_pipeline.stores.timescale import TimescaleWarehouse
from markets_pipeline.transforms.zscore import ZScoreTransform
from markets_pipeline.classifiers.rule_based import RuleBasedClassifier
from markets_core.domain.series import SeriesId


def main():
    """Run end-to-end test."""
    fred_key = os.environ.get("FRED_API_KEY")
    db_dsn = os.environ.get("DATABASE_URL") or "postgresql://postgres:your_password_here@localhost:5432/markets"
    
    if not fred_key:
        print("ERROR: FRED_API_KEY not set")
        return False
    
    print("=== Vertical Slice End-to-End Test ===")
    
    # Stage 1: Ingest
    print("\n[Stage 1] Ingesting DGS10 from FRED...")
    try:
        source = FredSource(api_key=fred_key)
        store = ParquetRawStore(path="/data/raw")
        
        # Health check
        health = source.health_check()
        print(f"  FRED API Health: {health}")
        
        series_id = SeriesId(source="fred", native_id="DGS10")
        
        # Fetch 30 days of data
        end = date.today()
        start = end - timedelta(days=30)
        
        observations = list(source.fetch(
            series_id=series_id,
            start=start,
            end=end,
        ))
        
        print(f"  Fetched {len(observations)} observations")
        
        if observations:
            store.append(
                source="fred",
                series_id=series_id,
                observations=observations,
                as_of=end,
            )
            print(f"  Wrote Parquet to /data/raw/fred/DGS10/{end}.parquet")
    except Exception as e:
        print(f"  ERROR: {e}")
        return False
    
    # Stage 2: Normalize
    print("\n[Stage 2] Normalizing into TimescaleDB...")
    try:
        warehouse = TimescaleWarehouse(dsn=db_dsn)
        
        raw_observations = list(store.read(source="fred", series_id=series_id))
        print(f"  Read {len(raw_observations)} observations from Parquet")
        
        if raw_observations:
            warehouse.upsert_observations(raw_observations)
            print(f"  Upserted into observations table")
    except Exception as e:
        print(f"  ERROR: {e}")
        return False
    
    # Stage 3: Derive
    print("\n[Stage 3] Computing z-scores...")
    try:
        transform = ZScoreTransform(warehouse=warehouse, window_days=63)
        
        zscores = transform.compute(series_id="fred:DGS10", as_of=end)
        print(f"  Computed {len(zscores)} z-scores")
        
        if zscores:
            warehouse.upsert_zscores(zscores)
            print(f"  Upserted z-scores into derived.zscores")
    except Exception as e:
        print(f"  ERROR: {e}")
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
            print(f"  Upserted regime states into regimes.regime_states")
    except Exception as e:
        print(f"  ERROR: {e}")
        return False
    
    print("\n✓ End-to-end test completed successfully!")
    return True


if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)