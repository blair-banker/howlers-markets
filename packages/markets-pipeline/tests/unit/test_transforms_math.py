"""Pure-math unit tests for transforms.

These test the numeric core of each transform in isolation from the DB.
"""
from datetime import date, timedelta
from decimal import Decimal
import pytest

from markets_pipeline.transforms.zscore import compute_zscore
from markets_pipeline.transforms.yoy_change import compute_yoy_pct
from markets_pipeline.transforms.trend import compute_trend_slope


def _series(values):
    """Produce (date, Decimal) pairs, one per calendar day ending 2026-04-15."""
    end = date(2026, 4, 15)
    return [(end - timedelta(days=len(values) - 1 - i), Decimal(str(v)))
            for i, v in enumerate(values)]


def test_zscore_constant_series_is_none_or_nan():
    daily = _series([1.0] * 100)
    assert compute_zscore(daily, window_days=63) is None


def test_zscore_known_values():
    daily = _series(list(range(1, 101)))
    z = compute_zscore(daily, window_days=63)
    assert z is not None
    assert Decimal("1.5") < z < Decimal("2.0")


def test_zscore_insufficient_history_returns_none():
    daily = _series([1.0] * 10)
    assert compute_zscore(daily, window_days=63) is None


def test_yoy_pct_basic():
    end = date(2026, 4, 15)
    values = {(end - timedelta(days=365)): Decimal("100"), end: Decimal("110")}
    y = compute_yoy_pct(values, end)
    assert y == Decimal("0.10")


def test_yoy_pct_missing_one_year_ago_returns_none():
    end = date(2026, 4, 15)
    values = {end: Decimal("110")}
    assert compute_yoy_pct(values, end) is None


def test_yoy_pct_tolerates_weekend_offset():
    end = date(2026, 4, 15)
    nearby = end - timedelta(days=368)
    values = {nearby: Decimal("100"), end: Decimal("110")}
    y = compute_yoy_pct(values, end, max_offset_days=7)
    assert y == Decimal("0.10")


def test_trend_slope_monotone_up_is_positive():
    daily = _series(list(range(1, 101)))
    slope = compute_trend_slope(daily, window_days=63)
    assert slope is not None and slope > Decimal("0")


def test_trend_slope_monotone_down_is_negative():
    daily = _series(list(range(100, 0, -1)))
    slope = compute_trend_slope(daily, window_days=63)
    assert slope is not None and slope < Decimal("0")


def test_trend_slope_flat_is_zero():
    daily = _series([5.0] * 100)
    slope = compute_trend_slope(daily, window_days=63)
    assert slope == Decimal("0")


def test_trend_slope_insufficient_history_returns_none():
    daily = _series([1.0, 2.0])
    assert compute_trend_slope(daily, window_days=63) is None
