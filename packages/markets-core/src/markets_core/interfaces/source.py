from typing import Protocol, Iterator
from datetime import date
from ..domain.series import SeriesId
from ..domain.observation import Observation


class Source(Protocol):
    source_name: str

    def fetch(
        self,
        series_id: SeriesId,
        start: date,
        end: date,
        as_of: date | None = None,
    ) -> Iterator[Observation]:
        """Fetch observations."""
        ...

    def health_check(self) -> dict:
        """Verify reachability."""
        ...

    def metadata(self, series_id: SeriesId) -> dict:
        """Describe a series."""
        ...