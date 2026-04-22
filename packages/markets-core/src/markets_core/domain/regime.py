from __future__ import annotations
from datetime import date
from decimal import Decimal
from typing import Annotated, Any, Literal, Union

from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
    TypeAdapter,
    field_validator,
    model_validator,
)


Metric = Literal["zscore", "yoy_change", "trend", "raw_value"]
Operator = Literal[">", ">=", "<", "<=", "=="]


class MetricRef(BaseModel):
    """Reference to a single metric value, used inside CompareSpreadNode."""
    model_config = ConfigDict(frozen=True, extra="forbid")

    metric: Metric
    variable: str
    window_days: int | None = None

    @model_validator(mode="after")
    def _window_required_for_windowed_metrics(self) -> "MetricRef":
        needs_window = self.metric in ("zscore", "trend")
        if needs_window and self.window_days is None:
            raise ValueError(f"metric={self.metric} requires window_days")
        if not needs_window and self.window_days is not None:
            raise ValueError(f"metric={self.metric} must not have window_days")
        return self


class CompareNode(BaseModel):
    """Leaf: compare a single metric against a scalar."""
    model_config = ConfigDict(frozen=True, extra="forbid")

    type: Literal["compare"]
    metric: Metric
    variable: str
    window_days: int | None
    op: Operator
    value: Decimal

    @model_validator(mode="after")
    def _window_consistent_with_metric(self) -> "CompareNode":
        needs_window = self.metric in ("zscore", "trend")
        if needs_window and self.window_days is None:
            raise ValueError(f"metric={self.metric} requires window_days")
        if not needs_window and self.window_days is not None:
            raise ValueError(f"metric={self.metric} must not have window_days")
        return self


class CompareSpreadNode(BaseModel):
    """Leaf: compare (left - right) against a scalar."""
    model_config = ConfigDict(frozen=True, extra="forbid")

    type: Literal["compare_spread"]
    left: MetricRef
    right: MetricRef
    op: Operator
    value: Decimal


class AllOfNode(BaseModel):
    """Compound: all child conditions must hold."""
    model_config = ConfigDict(frozen=True, extra="forbid")

    type: Literal["all_of"]
    conditions: list["ConditionNode"] = Field(min_length=1)


class AnyOfNode(BaseModel):
    """Compound: at least one child condition must hold."""
    model_config = ConfigDict(frozen=True, extra="forbid")

    type: Literal["any_of"]
    conditions: list["ConditionNode"] = Field(min_length=1)


ConditionNode = Annotated[
    Union[CompareNode, CompareSpreadNode, AllOfNode, AnyOfNode],
    Field(discriminator="type"),
]

# Rebuild forward refs
AllOfNode.model_rebuild()
AnyOfNode.model_rebuild()

_condition_adapter: TypeAdapter[ConditionNode] = TypeAdapter(ConditionNode)


def parse_condition(payload: Any) -> ConditionNode:
    """Parse a JSON-compatible dict into a validated ConditionNode."""
    return _condition_adapter.validate_python(payload)


class Regime(BaseModel):
    model_config = ConfigDict(frozen=True)

    id: int
    name: str
    display_name: str
    description: str
    tier: int


class RegimeTrigger(BaseModel):
    model_config = ConfigDict(frozen=True)

    id: int
    regime_id: int
    variable_id: int
    condition: ConditionNode
    weight: Decimal = Field(ge=Decimal("0"), le=Decimal("1"))
    description: str | None = None

    @field_validator("condition", mode="before")
    @classmethod
    def _parse_condition(cls, v: Any) -> Any:
        # Accept a pre-parsed model, a dict, or a JSON string from the DB
        if isinstance(v, str):
            import json
            v = json.loads(v)
        return v


class TriggerEvaluation(BaseModel):
    """Per-trigger evaluation record, for rationale_detail.triggers[]."""
    model_config = ConfigDict(frozen=True)

    regime: str
    trigger_id: int
    weight: Decimal
    satisfied: bool
    metric: str | None = None
    variable: str | None = None
    window_days: int | None = None
    value: Decimal | None = None
    op: str | None = None
    threshold: Decimal | None = None
    description: str | None = None


class RegimeClassification(BaseModel):
    """Classifier output for a single as-of date."""
    model_config = ConfigDict(frozen=True)

    as_of_date: date
    regime_name: str
    confidence: Decimal = Field(ge=Decimal("0"), le=Decimal("1"))
    trigger_variables: list[str]
    rationale: str
    rationale_detail: dict[str, Any]
    classifier_name: str
    classifier_version: str
    ontology_version: str
