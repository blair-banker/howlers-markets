import pandas as pd
import os
from pathlib import Path
from datetime import date
from typing import Iterator
import logging

from markets_core.domain.observation import Observation
from markets_core.domain.series import SeriesId
from markets_core.errors import StorageError

logger = logging.getLogger(__name__)


class ParquetRawStore:
    """Store raw observations as immutable Parquet files."""
    
    def __init__(self, path: str | Path):
        self.root_path = Path(path)
        self.root_path.mkdir(parents=True, exist_ok=True)
    
    def append(
        self,
        source: str,
        series_id: SeriesId,
        observations: list[Observation],
        as_of: date,
    ) -> None:
        """Append observations to raw storage (write-once)."""
        try:
            # Create path: data/raw/{source}/{native_id}/{as_of_date}.parquet
            series_path = self.root_path / source / series_id.native_id
            series_path.mkdir(parents=True, exist_ok=True)
            
            file_path = series_path / f"{as_of}.parquet"
            
            # Convert to DataFrame
            data = []
            for obs in observations:
                data.append({
                    "source": obs.series_id.split(":")[0],
                    "native_id": obs.series_id.split(":")[1],
                    "observation_date": obs.observation_date,
                    "as_of_date": obs.as_of_date,
                    "value": obs.value,
                    "ingested_at": obs.ingested_at,
                    "source_revision": obs.source_revision,
                })
            
            df = pd.DataFrame(data)
            df.to_parquet(file_path, index=False)
            
            logger.info(
                "parquet_append_completed",
                source=source,
                series_id=str(series_id),
                observations=len(observations),
                as_of=as_of,
                path=str(file_path),
            )
        except Exception as e:
            raise StorageError(
                f"Failed to append to Parquet: {e}",
                source=source,
                series_id=str(series_id),
            ) from e
    
    def read(self, source: str, series_id: SeriesId) -> Iterator[Observation]:
        """Read all observations for a series."""
        try:
            series_path = self.root_path / source / series_id.native_id
            
            if not series_path.exists():
                return
            
            # Read all Parquet files in chronological order
            parquet_files = sorted(series_path.glob("*.parquet"))
            
            for file_path in parquet_files:
                df = pd.read_parquet(file_path)
                
                for _, row in df.iterrows():
                    yield Observation(
                        series_id=f"{row['source']}:{row['native_id']}",
                        observation_date=row["observation_date"],
                        as_of_date=row["as_of_date"],
                        value=row["value"],
                        ingested_at=row["ingested_at"],
                        source_revision=row["source_revision"],
                    )
        except Exception as e:
            raise StorageError(
                f"Failed to read from Parquet: {e}",
                source=source,
                series_id=str(series_id),
            ) from e