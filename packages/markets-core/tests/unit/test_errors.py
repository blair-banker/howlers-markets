import pytest
from markets_core.errors import (
    MarketsCoreError,
    InsufficientDataError,
    ConfigurationError,
)


def test_insufficient_data_error_is_markets_core_error():
    err = InsufficientDataError("no data for variable 'x' at as-of 2026-04-01")
    assert isinstance(err, MarketsCoreError)


def test_insufficient_data_error_carries_message():
    err = InsufficientDataError("missing variable foo")
    assert "missing variable foo" in str(err)
