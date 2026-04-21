from typing import Protocol, Iterator
from datetime import date
from pathlib import Path
from ..domain.series import SeriesId
from ..domain.observation import Observation


class RawStore(Protocol):
    def append(self, source: str, series_id: SeriesId, observations: list[Observation], as_of: date) -> None:
        """Append observations to raw storage."""
        ...

    def read(self, source: str, series_id: SeriesId) -> Iterator[Observation]:
        """Read observations from raw storage."""
        ...


class Warehouse(Protocol):
    def upsert_observations(self, observations: list[Observation]) -> None:
        """Upsert observations into warehouse."""
        ...

    def query_point_in_time(self, series_ids: list[str], as_of: date) -> dict:
        """Query point-in-time observations."""
        ...