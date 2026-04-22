import requests
import os
from datetime import date, datetime, timedelta
from typing import Iterator
from decimal import Decimal
import logging

from markets_core.domain.observation import Observation
from markets_core.domain.series import SeriesId
from markets_core.errors import DataSourceError, SeriesNotFoundError

logger = logging.getLogger(__name__)


class FredSource:
    """Fetch data from Federal Reserve Economic Data (FRED)."""
    
    source_name = "fred"
    
    def __init__(self, api_key: str):
        self.api_key = api_key
        self.base_url = "https://api.stlouisfed.org/fred"
    
    def fetch(
        self,
        series_id: SeriesId,
        start: date,
        end: date,
        as_of: date | None = None,
    ) -> Iterator[Observation]:
        """Fetch observations from FRED API."""
        if series_id.source != "fred":
            raise ValueError(f"Series {series_id} is not a FRED series")
        
        try:
            url = f"{self.base_url}/series/data"
            params = {
                "series_id": series_id.native_id,
                "api_key": self.api_key,
                "file_type": "json",
                "observation_start": start.isoformat(),
                "observation_end": end.isoformat(),
            }
            
            response = requests.get(url, params=params, timeout=10)
            response.raise_for_status()
            data = response.json()
            
            if "error_code" in data:
                if data["error_code"] == 400:
                    raise SeriesNotFoundError(
                        f"Series {series_id.native_id} not found at FRED",
                        source="fred",
                        series_id=str(series_id),
                    )
                raise DataSourceError(
                    f"FRED error: {data.get('error_message', 'unknown')}",
                    source="fred",
                    series_id=str(series_id),
                )
            
            observations = data.get("observations", [])
            as_of_date = as_of or date.today()
            
            for obs in observations:
                obs_date = date.fromisoformat(obs["date"])
                value = obs.get("value")
                
                if value == "." or value is None:
                    continue
                
                yield Observation(
                    series_id=str(series_id),
                    observation_date=obs_date,
                    as_of_date=as_of_date,
                    value=Decimal(value),
                    ingested_at=datetime.now(),
                    source_revision=None,
                )
                
        except requests.RequestException as e:
            raise DataSourceError(
                f"FRED request failed: {e}",
                source="fred",
                series_id=str(series_id),
            ) from e
    
    def health_check(self) -> dict:
        """Verify API connectivity."""
        try:
            url = f"{self.base_url}/series/data"
            params = {
                "series_id": "DGS10",
                "api_key": self.api_key,
                "limit": 1,
                "file_type": "json",
            }
            response = requests.get(url, params=params, timeout=5)
            response.raise_for_status()
            return {"healthy": True, "message": "FRED API reachable"}
        except Exception as e:
            return {"healthy": False, "message": str(e)}
    
    def metadata(self, series_id: SeriesId) -> dict:
        """Fetch series metadata."""
        if series_id.source != "fred":
            raise ValueError(f"Series {series_id} is not a FRED series")
        
        try:
            url = f"{self.base_url}/series"
            params = {
                "series_id": series_id.native_id,
                "api_key": self.api_key,
                "file_type": "json",
            }
            response = requests.get(url, params=params, timeout=10)
            response.raise_for_status()
            data = response.json()
            
            if "error_code" in data:
                raise SeriesNotFoundError(
                    f"Series {series_id.native_id} not found",
                    source="fred",
                    series_id=str(series_id),
                )
            
            series = data.get("seriess", [{}])[0]
            return {
                "title": series.get("title", ""),
                "units": series.get("units", ""),
                "frequency": series.get("frequency", ""),
            }
        except requests.RequestException as e:
            raise DataSourceError(
                f"FRED metadata request failed: {e}",
                source="fred",
                series_id=str(series_id),
            ) from e