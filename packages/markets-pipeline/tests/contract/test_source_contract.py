import pytest
from typing import Iterator
from datetime import date
from markets_core.interfaces.source import Source
from markets_core.domain.observation import Observation
from markets_core.domain.series import SeriesId


class SourceContract:
    """Shared contract every Source implementation must satisfy."""

    @pytest.fixture
    def source(self) -> Source:
        raise NotImplementedError("Override in subclass")

    @pytest.fixture
    def known_series_id(self) -> SeriesId:
        raise NotImplementedError("Override in subclass")

    def test_fetch_returns_iterator(self, source, known_series_id):
        result = source.fetch(known_series_id, date(2026, 1, 1), date(2026, 1, 31))
        assert hasattr(result, "__iter__")

    def test_fetch_observations_have_valid_dates(self, source, known_series_id):
        for obs in source.fetch(known_series_id, date(2026, 1, 1), date(2026, 1, 31)):
            assert obs.observation_date <= obs.as_of_date