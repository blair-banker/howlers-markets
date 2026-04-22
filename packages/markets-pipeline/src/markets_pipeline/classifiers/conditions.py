"""Pure-function condition evaluator for regime triggers.

Takes a validated ConditionNode and a flat feature dict
(key format: "{metric}:{variable}:{window_days or ''}") and returns bool.
Missing or None feature values raise InsufficientDataError.
"""
from __future__ import annotations
from decimal import Decimal
from typing import Iterable

from markets_core.domain.regime import (
    AllOfNode,
    AnyOfNode,
    CompareNode,
    CompareSpreadNode,
    ConditionNode,
    MetricRef,
)
from markets_core.errors import InsufficientDataError


def feature_key(metric: str, variable: str, window_days: int | None) -> str:
    w = str(window_days) if window_days is not None else ""
    return f"{metric}:{variable}:{w}"


def _lookup(features: dict[str, Decimal | None], key: str) -> Decimal:
    if key not in features:
        raise InsufficientDataError(f"feature missing: {key}")
    v = features[key]
    if v is None:
        raise InsufficientDataError(f"feature is null: {key}")
    return v


def _apply_op(op: str, lhs: Decimal, rhs: Decimal) -> bool:
    if op == ">":
        return lhs > rhs
    if op == ">=":
        return lhs >= rhs
    if op == "<":
        return lhs < rhs
    if op == "<=":
        return lhs <= rhs
    if op == "==":
        return lhs == rhs
    raise ValueError(f"unknown operator: {op}")


def _metric_ref_key(ref: MetricRef) -> str:
    return feature_key(ref.metric, ref.variable, ref.window_days)


def evaluate(node: ConditionNode, features: dict[str, Decimal | None]) -> bool:
    if isinstance(node, CompareNode):
        lhs = _lookup(features, feature_key(node.metric, node.variable, node.window_days))
        return _apply_op(node.op, lhs, node.value)
    if isinstance(node, CompareSpreadNode):
        left = _lookup(features, _metric_ref_key(node.left))
        right = _lookup(features, _metric_ref_key(node.right))
        return _apply_op(node.op, left - right, node.value)
    if isinstance(node, AllOfNode):
        return all(evaluate(c, features) for c in node.conditions)
    if isinstance(node, AnyOfNode):
        return any(evaluate(c, features) for c in node.conditions)
    raise TypeError(f"unknown node type: {type(node).__name__}")


def required_features(node: ConditionNode) -> Iterable[str]:
    """Yield every feature key the node will look up during evaluation."""
    if isinstance(node, CompareNode):
        yield feature_key(node.metric, node.variable, node.window_days)
    elif isinstance(node, CompareSpreadNode):
        yield _metric_ref_key(node.left)
        yield _metric_ref_key(node.right)
    elif isinstance(node, (AllOfNode, AnyOfNode)):
        for c in node.conditions:
            yield from required_features(c)
    else:
        raise TypeError(f"unknown node type: {type(node).__name__}")
