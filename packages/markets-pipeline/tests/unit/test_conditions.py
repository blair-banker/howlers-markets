from decimal import Decimal
import pytest

from markets_core.domain.regime import (
    CompareNode, CompareSpreadNode, AllOfNode, AnyOfNode, MetricRef,
)
from markets_core.errors import InsufficientDataError

from markets_pipeline.classifiers.conditions import (
    evaluate,
    feature_key,
    required_features,
)


def _cmp(metric, var, window, op, val):
    return CompareNode(type="compare", metric=metric, variable=var,
                       window_days=window, op=op, value=Decimal(val))


def test_feature_key_includes_window():
    assert feature_key("zscore", "brent_oil", 63) == "zscore:brent_oil:63"
    assert feature_key("raw_value", "us_real_10y", None) == "raw_value:us_real_10y:"


def test_compare_gt_true():
    node = _cmp("zscore", "brent_oil", 63, ">", "2.0")
    features = {"zscore:brent_oil:63": Decimal("2.1")}
    assert evaluate(node, features) is True


def test_compare_gt_false():
    node = _cmp("zscore", "brent_oil", 63, ">", "2.0")
    features = {"zscore:brent_oil:63": Decimal("1.9")}
    assert evaluate(node, features) is False


def test_compare_gte_boundary():
    node = _cmp("raw_value", "us_real_10y", None, ">=", "0")
    assert evaluate(node, {"raw_value:us_real_10y:": Decimal("0")}) is True


def test_compare_lt_true():
    node = _cmp("zscore", "dxy", 63, "<", "-1.0")
    assert evaluate(node, {"zscore:dxy:63": Decimal("-1.5")}) is True


def test_compare_equal():
    node = _cmp("raw_value", "x", None, "==", "0")
    assert evaluate(node, {"raw_value:x:": Decimal("0")}) is True
    assert evaluate(node, {"raw_value:x:": Decimal("0.01")}) is False


def test_compare_raises_on_missing_feature():
    node = _cmp("zscore", "brent_oil", 63, ">", "2.0")
    with pytest.raises(InsufficientDataError):
        evaluate(node, {})


def test_compare_raises_on_none_feature():
    node = _cmp("zscore", "brent_oil", 63, ">", "2.0")
    with pytest.raises(InsufficientDataError):
        evaluate(node, {"zscore:brent_oil:63": None})


def test_compare_spread_gt_true():
    node = CompareSpreadNode(
        type="compare_spread",
        left=MetricRef(metric="zscore", variable="headline_cpi_yoy", window_days=63),
        right=MetricRef(metric="zscore", variable="core_cpi_yoy", window_days=63),
        op=">",
        value=Decimal("0.5"),
    )
    features = {
        "zscore:headline_cpi_yoy:63": Decimal("2.0"),
        "zscore:core_cpi_yoy:63": Decimal("1.0"),
    }
    assert evaluate(node, features) is True


def test_compare_spread_raises_when_either_side_missing():
    node = CompareSpreadNode(
        type="compare_spread",
        left=MetricRef(metric="zscore", variable="a", window_days=63),
        right=MetricRef(metric="zscore", variable="b", window_days=63),
        op=">",
        value=Decimal("0.5"),
    )
    with pytest.raises(InsufficientDataError):
        evaluate(node, {"zscore:a:63": Decimal("1.0")})


def test_all_of_all_true():
    n1 = _cmp("raw_value", "x", None, ">", "0")
    n2 = _cmp("raw_value", "y", None, ">", "0")
    node = AllOfNode(type="all_of", conditions=[n1, n2])
    assert evaluate(node, {"raw_value:x:": Decimal("1"), "raw_value:y:": Decimal("1")}) is True


def test_all_of_one_false():
    n1 = _cmp("raw_value", "x", None, ">", "0")
    n2 = _cmp("raw_value", "y", None, ">", "0")
    node = AllOfNode(type="all_of", conditions=[n1, n2])
    assert evaluate(node, {"raw_value:x:": Decimal("1"), "raw_value:y:": Decimal("-1")}) is False


def test_any_of_one_true():
    n1 = _cmp("raw_value", "x", None, ">", "0")
    n2 = _cmp("raw_value", "y", None, ">", "0")
    node = AnyOfNode(type="any_of", conditions=[n1, n2])
    assert evaluate(node, {"raw_value:x:": Decimal("-1"), "raw_value:y:": Decimal("1")}) is True


def test_any_of_all_false():
    n1 = _cmp("raw_value", "x", None, ">", "0")
    n2 = _cmp("raw_value", "y", None, ">", "0")
    node = AnyOfNode(type="any_of", conditions=[n1, n2])
    assert evaluate(node, {"raw_value:x:": Decimal("-1"), "raw_value:y:": Decimal("-1")}) is False


def test_required_features_enumerates_all_leaves():
    n1 = _cmp("zscore", "a", 63, ">", "0")
    n2 = CompareSpreadNode(
        type="compare_spread",
        left=MetricRef(metric="yoy_change", variable="b"),
        right=MetricRef(metric="yoy_change", variable="c"),
        op=">", value=Decimal("0"),
    )
    node = AllOfNode(type="all_of", conditions=[n1, n2])
    keys = set(required_features(node))
    assert keys == {
        "zscore:a:63",
        "yoy_change:b:",
        "yoy_change:c:",
    }
