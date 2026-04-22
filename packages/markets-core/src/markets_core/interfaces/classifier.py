from __future__ import annotations
from datetime import date
from typing import Protocol, runtime_checkable

from ..domain.regime import RegimeClassification


@runtime_checkable
class Classifier(Protocol):
    """Protocol for regime classifiers.

    Implementations MUST:
    - Read data point-in-time (no use of observations with as_of_date > as_of_date arg).
    - Be idempotent: two calls with the same as_of_date and unchanged ontology
      return identical RegimeClassification output.
    - Raise InsufficientDataError when required data is missing at as_of_date.
    - Raise ConfigurationError if the ontology has malformed trigger conditions.
    """

    name: str
    version: str

    def classify(self, as_of_date: date) -> RegimeClassification:
        """Produce a regime classification for the given as-of date."""
        ...
