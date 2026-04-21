from typing import Protocol
from datetime import date


class Transform(Protocol):
    def compute(self, as_of_date: date) -> None:
        """Compute derived values for the given as-of date."""
        ...