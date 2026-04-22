import numpy as np
import pandas as pd
from datetime import date, timedelta
from decimal import Decimal
import logging

logger = logging.getLogger(__name__)


class ZScoreTransform:
    """Compute rolling z-scores for a series."""
    
    def __init__(self, warehouse, window_days: int = 63):
        """Initialize with warehouse connection and window size."""
        self.warehouse = warehouse
        self.window_days = window_days
    
    def compute(self, series_id: str, as_of: date) -> list[dict]:
        """Compute z-scores for a series as of a date."""
        try:
            # Query point-in-time observations
            data = self.warehouse.query_point_in_time([series_id], as_of)
            
            if series_id not in data or not data[series_id]:
                logger.warning(
                    "no_observations_for_zscore",
                    series_id=series_id,
                    as_of=as_of,
                )
                return []
            
            # Convert to DataFrame
            obs_list = data[series_id]
            df = pd.DataFrame(obs_list)
            df["observation_date"] = pd.to_datetime(df["observation_date"])
            df["value"] = df["value"].astype(float)
            df = df.sort_values("observation_date")
            
            # Compute rolling z-scores
            zscores = []
            
            for i, row in df.iterrows():
                obs_date = row["observation_date"].date()
                value = row["value"]
                
                # Look back window_days for rolling mean/std
                window_start = obs_date - timedelta(days=self.window_days)
                window_df = df[
                    (df["observation_date"].dt.date >= window_start) &
                    (df["observation_date"].dt.date <= obs_date)
                ]
                
                if len(window_df) < 2:
                    # Not enough data for z-score
                    continue
                
                mean = window_df["value"].mean()
                std = window_df["value"].std()
                
                if std == 0:
                    zscore = 0.0
                else:
                    zscore = (value - mean) / std
                
                zscores.append({
                    "series_id": series_id,
                    "observation_date": obs_date,
                    "as_of_date": as_of,
                    "window_days": self.window_days,
                    "zscore": Decimal(str(round(zscore, 4))),
                })
            
            logger.info(
                "zscore_compute_completed",
                series_id=series_id,
                as_of=as_of,
                zscores=len(zscores),
            )
            
            return zscores
        except Exception as e:
            logger.error(
                "zscore_compute_failed",
                series_id=series_id,
                as_of=as_of,
                error=str(e),
            )
            raise