from decimal import Decimal
from datetime import date
import pytest
from pydantic import ValidationError

from markets_core.domain.regime import (
    Regime,
    RegimeTrigger,
    ConditionNode,
    CompareNode,
    CompareSpreadNode,
    AllOfNode,
    AnyOfNode,
    MetricRef,
    RegimeClassification,
    TriggerEvaluation,
)


def test_compare_node_parses_valid():
    node = CompareNode(
        type="compare",
        metric="zscore",
        variable="brent_oil",
        window_days=63,
        op=">",
        value=Decimal("2.0"),
    )
    assert node.metric == "zscore"
    assert node.op == ">"


def test_compare_node_rejects_unknown_metric():
    with pytest.raises(ValidationError):
        CompareNode(
            type="compare",
            metric="not_a_real_metric",
            variable="x",
            window_days=63,
            op=">",
            value=Decimal("1.0"),
        )


def test_compare_node_rejects_unknown_operator():
    with pytest.raises(ValidationError):
        CompareNode(
            type="compare",
            metric="zscore",
            variable="x",
            window_days=63,
            op="=~",
            value=Decimal("1.0"),
        )


def test_compare_node_requires_window_for_zscore():
    with pytest.raises(ValidationError):
        CompareNode(
            type="compare",
            metric="zscore",
            variable="x",
            window_days=None,
            op=">",
            value=Decimal("1.0"),
        )


def test_compare_node_rejects_window_for_raw_value():
    with pytest.raises(ValidationError):
        CompareNode(
            type="compare",
            metric="raw_value",
            variable="x",
            window_days=63,
            op=">",
            value=Decimal("0"),
        )


def test_compare_spread_node_parses_valid():
    node = CompareSpreadNode(
        type="compare_spread",
        left=MetricRef(metric="zscore", variable="headline_cpi_yoy", window_days=63),
        right=MetricRef(metric="zscore", variable="core_cpi_yoy", window_days=63),
        op=">",
        value=Decimal("0.5"),
    )
    assert node.op == ">"


def test_all_of_node_requires_non_empty_conditions():
    inner = CompareNode(type="compare", metric="raw_value", variable="x",
                        window_days=None, op=">", value=Decimal("0"))
    node = AllOfNode(type="all_of", conditions=[inner])
    assert len(node.conditions) == 1
    with pytest.raises(ValidationError):
        AllOfNode(type="all_of", conditions=[])


def test_any_of_node_requires_non_empty_conditions():
    with pytest.raises(ValidationError):
        AnyOfNode(type="any_of", conditions=[])


def test_condition_node_discriminated_union_round_trip():
    payload = {
        "type": "all_of",
        "conditions": [
            {"type": "compare", "metric": "zscore", "variable": "brent_oil",
             "window_days": 63, "op": ">", "value": "2.0"},
            {"type": "compare_spread",
             "left": {"metric": "zscore", "variable": "a", "window_days": 63},
             "right": {"metric": "zscore", "variable": "b", "window_days": 63},
             "op": ">", "value": "0.5"},
        ],
    }
    from markets_core.domain.regime import parse_condition
    node = parse_condition(payload)
    assert isinstance(node, AllOfNode)
    dumped = node.model_dump(mode="json")
    again = parse_condition(dumped)
    assert again == node


def test_regime_trigger_accepts_structured_condition():
    cond = CompareNode(type="compare", metric="raw_value", variable="x",
                       window_days=None, op=">", value=Decimal("0"))
    trigger = RegimeTrigger(
        id=1, regime_id=2, variable_id=3,
        condition=cond, weight=Decimal("0.4"), description=None,
    )
    assert trigger.weight == Decimal("0.4")


def test_regime_trigger_rejects_weight_out_of_range():
    cond = CompareNode(type="compare", metric="raw_value", variable="x",
                       window_days=None, op=">", value=Decimal("0"))
    with pytest.raises(ValidationError):
        RegimeTrigger(id=1, regime_id=2, variable_id=3,
                      condition=cond, weight=Decimal("-0.1"), description=None)
    with pytest.raises(ValidationError):
        RegimeTrigger(id=1, regime_id=2, variable_id=3,
                      condition=cond, weight=Decimal("1.5"), description=None)


def test_regime_classification_confidence_bounded():
    with pytest.raises(ValidationError):
        RegimeClassification(
            as_of_date=date(2026, 4, 15),
            regime_name="benign_expansion",
            confidence=Decimal("1.5"),
            trigger_variables=[],
            rationale="",
            rationale_detail={},
            classifier_name="rule_based",
            classifier_version="1.0.0",
            ontology_version="002",
        )
