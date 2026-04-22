"""Contract tests for YfinanceSource.

Requires internet. Marked @pytest.mark.integration.
"""
from datetime import date

import pytest

from markets_core.domain.series import SeriesId
from markets_core.errors import SeriesNotFoundError

from markets_pipeline.sources.yfinance import YfinanceSource


pytestmark = pytest.mark.integration


def test_fetch_returns_iterator():
    src = YfinanceSource()
    result = src.fetch(
        SeriesId(source="yfinance", native_id="BZ=F:close"),
        start=date(2024, 3, 1),
        end=date(2024, 3, 15),
    )
    assert hasattr(result, "__iter__")


def test_fetch_observations_have_valid_dates():
    src = YfinanceSource()
    obs_list = list(src.fetch(
        SeriesId(source="yfinance", native_id="BZ=F:close"),
        start=date(2024, 3, 1),
        end=date(2024, 3, 15),
    ))
    assert len(obs_list) > 0
    for obs in obs_list:
        assert obs.observation_date <= obs.as_of_date
        assert obs.observation_date >= date(2024, 3, 1)
        assert obs.observation_date < date(2024, 3, 15)


def test_fetch_rejects_unknown_ticker():
    src = YfinanceSource()
    with pytest.raises(SeriesNotFoundError):
        list(src.fetch(
            SeriesId(source="yfinance", native_id="NOT_A_REAL:close"),
            start=date(2024, 3, 1),
            end=date(2024, 3, 15),
        ))


def test_fetch_rejects_bad_native_id_format():
    src = YfinanceSource()
    with pytest.raises(SeriesNotFoundError):
        list(src.fetch(
            SeriesId(source="yfinance", native_id="BZ=F"),
            start=date(2024, 3, 1),
            end=date(2024, 3, 15),
        ))


def test_health_check_returns_status():
    status = YfinanceSource().health_check()
    assert "healthy" in status


def test_metadata_for_valid_series():
    meta = YfinanceSource().metadata(
        SeriesId(source="yfinance", native_id="DX-Y.NYB:close")
    )
    assert meta["frequency"] == "daily"
    assert meta["channel"] == "close"


def test_metadata_raises_for_unknown_ticker():
    with pytest.raises(SeriesNotFoundError):
        YfinanceSource().metadata(
            SeriesId(source="yfinance", native_id="NOT_A_REAL:close")
        )
