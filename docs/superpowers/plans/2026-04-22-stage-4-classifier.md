# Stage 4 Rule-Based Classifier Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Ship a working Stage 4 rule-based classifier that distinguishes four market regimes (`benign_expansion`, `monetary_tightening`, `monetary_easing`, `supply_shock`) with strict point-in-time discipline, plus the minimum Stage 3 derived views and yfinance source needed to support it.

**Architecture:** Three layers — abstractions in `markets-core` (regime domain + `Classifier` protocol + contract suite), implementations in `markets-pipeline` (three `Transform` implementations for z-score/YoY/trend, `RuleBasedClassifier`, minimal `YfinanceSource`, Airflow DAG), and schema changes in `infrastructure/sql` (additive migration + populated ontology seed). The classifier reads derived views point-in-time, evaluates JSON-AST trigger conditions, and produces weighted-score regime classifications with a structured rationale.

**Tech Stack:** Python 3.11+, Pydantic v2, Apache Airflow 2.7+, TimescaleDB, Alembic, psycopg (v3), pytest, hypothesis, yfinance.

**Spec:** `docs/superpowers/specs/2026-04-22-stage-4-classifier-design.md`

---

## File Structure

### Create
- `packages/markets-core/src/markets_core/domain/regime.py` — Pydantic models: `Regime`, `RegimeTrigger`, `ConditionNode` (discriminated union), `RegimeClassification`, `TriggerEvaluation`.
- `packages/markets-core/src/markets_core/interfaces/classifier.py` — `Classifier` protocol.
- `packages/markets-core/tests/contract/test_classifier_contract.py` — shared contract suite for `Classifier`.
- `packages/markets-core/tests/contract/__init__.py` — empty package marker.
- `packages/markets-core/tests/unit/test_regime_models.py` — Pydantic validation tests.
- `packages/markets-pipeline/src/markets_pipeline/classifiers/__init__.py` — empty package marker.
- `packages/markets-pipeline/src/markets_pipeline/classifiers/conditions.py` — pure-function condition evaluator.
- `packages/markets-pipeline/src/markets_pipeline/classifiers/rule_based.py` — `RuleBasedClassifier`.
- `packages/markets-pipeline/src/markets_pipeline/transforms/__init__.py` — empty package marker.
- `packages/markets-pipeline/src/markets_pipeline/transforms/zscore.py` — `ZScoreTransform`.
- `packages/markets-pipeline/src/markets_pipeline/transforms/yoy_change.py` — `YoYChangeTransform`.
- `packages/markets-pipeline/src/markets_pipeline/transforms/trend.py` — `TrendTransform`.
- `packages/markets-pipeline/src/markets_pipeline/sources/yfinance.py` — `YfinanceSource`.
- `packages/markets-pipeline/src/markets_pipeline/dags/ingest_yfinance.py` — ingest DAG for DXY + Brent.
- `packages/markets-pipeline/tests/unit/test_conditions.py` — condition evaluator tests.
- `packages/markets-pipeline/tests/unit/test_rule_based_scoring.py` — scoring/tiebreak unit tests.
- `packages/markets-pipeline/tests/contract/test_rule_based_contract.py` — classifier contract subclass.
- `packages/markets-pipeline/tests/contract/test_transforms_contract.py` — transform contract subclasses.
- `packages/markets-pipeline/tests/contract/test_yfinance_contract.py` — source contract subclass.
- `packages/markets-pipeline/tests/integration/test_stage4_end_to_end.py` — seed → transform → classify smoke test.
- `packages/markets-pipeline/tests/integration/test_historical_regression.py` — three historical episodes.
- `scripts/backfill_classify.py` — explicit history-replay command.
- `infrastructure/sql/migrations/versions/002_stage4_classifier.py` — additive migration.

### Modify
- `packages/markets-core/src/markets_core/domain/ontology.py` — remove `Regime` and `RegimeTrigger` (relocated to `regime.py`); keep `Variable`.
- `packages/markets-core/src/markets_core/domain/__init__.py` — re-export regime models.
- `packages/markets-core/src/markets_core/interfaces/__init__.py` — export `Classifier`.
- `packages/markets-core/src/markets_core/errors.py` — add `InsufficientDataError`.
- `packages/markets-pipeline/src/markets_pipeline/stores/timescale.py` — add warehouse methods needed by transforms and classifier (see Task 4).
- `packages/markets-pipeline/src/markets_pipeline/dags/classify.py` — rewrite to use proper classifier with ontology version.
- `packages/markets-pipeline/src/markets_pipeline/dags/derive.py` — rewrite to run all three transforms.
- `infrastructure/sql/seed/ontology.sql` — replace with full 7-variable + 4-regime seed.
- `Makefile` — add `backfill-classify`, `test-stage4` targets.
- `CLAUDE.md` — add "No sub-daily data" non-negotiable rule.
- `docs/DATA-MODEL.md` — note the jsonb change and new columns.
- `docs/ONTOLOGY.md` — note that 4 regimes are seeded; others deferred.

### Responsibility boundaries
- `domain/regime.py`: shape only, no evaluation logic.
- `interfaces/classifier.py`: protocol only, no implementation.
- `classifiers/conditions.py`: pure evaluator — takes a `ConditionNode` and a `dict[str, Decimal | None]` feature row, returns `bool`. No I/O, no DB.
- `classifiers/rule_based.py`: reads ontology + derived views, calls conditions.py, produces `RegimeClassification`. No raw observation reads.
- `transforms/*.py`: read `observations` point-in-time (via warehouse), write one derived table each. No classifier knowledge.
- `stores/timescale.py`: all SQL lives here. Transforms and classifier depend on the warehouse interface, not on raw SQL.

---

## Task 1: Add `InsufficientDataError`

**Files:**
- Modify: `packages/markets-core/src/markets_core/errors.py`
- Test: `packages/markets-core/tests/unit/test_errors.py` (create if missing)

- [ ] **Step 1: Write the failing test**

Create or append `packages/markets-core/tests/unit/test_errors.py`:

```python
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
```

- [ ] **Step 2: Run test to verify it fails**

```bash
pytest packages/markets-core/tests/unit/test_errors.py -v
```

Expected: `ImportError` on `InsufficientDataError`.

- [ ] **Step 3: Add the exception**

In `packages/markets-core/src/markets_core/errors.py`, append after `ConfigurationError`:

```python
class InsufficientDataError(MarketsCoreError):
    """Required data for classification was not available at the given as-of date."""
    pass
```

- [ ] **Step 4: Run test to verify it passes**

```bash
pytest packages/markets-core/tests/unit/test_errors.py -v
```

Expected: both tests PASS.

- [ ] **Step 5: Commit**

```bash
git add packages/markets-core/src/markets_core/errors.py packages/markets-core/tests/unit/test_errors.py
git commit -m "feat(core): add InsufficientDataError exception"
```

---

## Task 2: Regime domain models + condition AST

**Files:**
- Create: `packages/markets-core/src/markets_core/domain/regime.py`
- Modify: `packages/markets-core/src/markets_core/domain/ontology.py` (remove Regime, RegimeTrigger)
- Modify: `packages/markets-core/src/markets_core/domain/__init__.py`
- Test: `packages/markets-core/tests/unit/test_regime_models.py`

This defines the shape of everything downstream. Note: `Regime` and `RegimeTrigger` already exist in `ontology.py` with `condition: str` — we relocate and retype them.

- [ ] **Step 1: Write the failing test**

Create `packages/markets-core/tests/unit/test_regime_models.py`:

```python
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
    # Parse from dict -> model -> dict, covering all node types
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
```

- [ ] **Step 2: Run test to verify it fails**

```bash
pytest packages/markets-core/tests/unit/test_regime_models.py -v
```

Expected: `ImportError` on `markets_core.domain.regime`.

- [ ] **Step 3: Create the domain module**

Create `packages/markets-core/src/markets_core/domain/regime.py`:

```python
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
        # Accept either a pre-parsed model, a dict, or a JSON string from the DB
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
    # Populated for compare leaves only (None for compound outer nodes)
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
```

- [ ] **Step 4: Update `domain/ontology.py`**

Edit `packages/markets-core/src/markets_core/domain/ontology.py` — remove the `Regime` and `RegimeTrigger` classes (they now live in `regime.py`). Keep `Variable`. Final contents:

```python
from pydantic import BaseModel
from typing import Optional


class Variable(BaseModel):
    id: int
    name: str
    display_name: str
    description: Optional[str] = None
    tier: int
    primary_series: Optional[str] = None
```

- [ ] **Step 5: Update `domain/__init__.py`**

Edit `packages/markets-core/src/markets_core/domain/__init__.py` to re-export. If the file is currently empty or minimal, write it as:

```python
from .observation import Observation
from .series import SeriesId
from .ontology import Variable
from .regime import (
    Regime,
    RegimeTrigger,
    ConditionNode,
    CompareNode,
    CompareSpreadNode,
    AllOfNode,
    AnyOfNode,
    MetricRef,
    TriggerEvaluation,
    RegimeClassification,
    parse_condition,
)

__all__ = [
    "Observation", "SeriesId", "Variable",
    "Regime", "RegimeTrigger",
    "ConditionNode", "CompareNode", "CompareSpreadNode",
    "AllOfNode", "AnyOfNode", "MetricRef",
    "TriggerEvaluation", "RegimeClassification", "parse_condition",
]
```

If the file already has different exports, preserve them and add the new ones.

- [ ] **Step 6: Run tests**

```bash
pytest packages/markets-core/tests/unit/test_regime_models.py -v
```

Expected: all PASS.

Also re-run any existing core tests to make sure the ontology relocation didn't break anything:

```bash
pytest packages/markets-core/tests/unit/ -v
```

Expected: all PASS.

- [ ] **Step 7: Commit**

```bash
git add packages/markets-core/src/markets_core/domain/regime.py \
        packages/markets-core/src/markets_core/domain/ontology.py \
        packages/markets-core/src/markets_core/domain/__init__.py \
        packages/markets-core/tests/unit/test_regime_models.py
git commit -m "feat(core): add regime domain models with JSON-AST condition types"
```

---

## Task 3: `Classifier` protocol + contract test suite

**Files:**
- Create: `packages/markets-core/src/markets_core/interfaces/classifier.py`
- Modify: `packages/markets-core/src/markets_core/interfaces/__init__.py`
- Create: `packages/markets-core/tests/contract/__init__.py`
- Create: `packages/markets-core/tests/contract/test_classifier_contract.py`

The contract suite is defined here but has no implementation to run against yet. It becomes active in Task 8 when `RuleBasedClassifier` subclasses it.

- [ ] **Step 1: Create the protocol**

Create `packages/markets-core/src/markets_core/interfaces/classifier.py`:

```python
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
```

- [ ] **Step 2: Re-export from `interfaces/__init__.py`**

Read the current contents of `packages/markets-core/src/markets_core/interfaces/__init__.py` and add `Classifier` to the exports. If the file is empty, write:

```python
from .source import Source
from .store import *  # noqa: F401, F403  (preserve whatever the file does now)
from .transform import Transform
from .classifier import Classifier

__all__ = ["Source", "Transform", "Classifier"]
```

If the file already imports specific names, just add `from .classifier import Classifier` and extend `__all__`.

- [ ] **Step 3: Write the contract test suite**

Create `packages/markets-core/tests/contract/__init__.py` (empty).

Create `packages/markets-core/tests/contract/test_classifier_contract.py`:

```python
"""Shared contract every Classifier implementation must satisfy.

Implementations subclass ClassifierContract and override:
- classifier_fixture: returns a ready-to-use Classifier
- known_good_as_of_date: a date for which the classifier should succeed
- known_insufficient_as_of_date: a date for which InsufficientDataError is expected

Implementations may mark methods @pytest.mark.integration if they need a DB.
"""
from __future__ import annotations
from datetime import date
from decimal import Decimal

import pytest

from markets_core.domain.regime import RegimeClassification
from markets_core.errors import InsufficientDataError
from markets_core.interfaces.classifier import Classifier


class ClassifierContract:
    @pytest.fixture
    def classifier(self) -> Classifier:
        raise NotImplementedError("Override in subclass")

    @pytest.fixture
    def known_good_as_of_date(self) -> date:
        raise NotImplementedError("Override in subclass")

    @pytest.fixture
    def known_insufficient_as_of_date(self) -> date:
        raise NotImplementedError("Override in subclass")

    def test_classify_returns_regime_classification(self, classifier, known_good_as_of_date):
        result = classifier.classify(known_good_as_of_date)
        assert isinstance(result, RegimeClassification)

    def test_classify_confidence_in_unit_interval(self, classifier, known_good_as_of_date):
        result = classifier.classify(known_good_as_of_date)
        assert Decimal("0") <= result.confidence <= Decimal("1")

    def test_classify_as_of_date_matches_input(self, classifier, known_good_as_of_date):
        result = classifier.classify(known_good_as_of_date)
        assert result.as_of_date == known_good_as_of_date

    def test_classify_classifier_name_and_version_match_attrs(self, classifier, known_good_as_of_date):
        result = classifier.classify(known_good_as_of_date)
        assert result.classifier_name == classifier.name
        assert result.classifier_version == classifier.version

    def test_classify_ontology_version_is_non_empty(self, classifier, known_good_as_of_date):
        result = classifier.classify(known_good_as_of_date)
        assert result.ontology_version != ""

    def test_classify_is_idempotent(self, classifier, known_good_as_of_date):
        a = classifier.classify(known_good_as_of_date)
        b = classifier.classify(known_good_as_of_date)
        assert a == b

    def test_classify_raises_on_insufficient_data(self, classifier, known_insufficient_as_of_date):
        with pytest.raises(InsufficientDataError):
            classifier.classify(known_insufficient_as_of_date)
```

- [ ] **Step 4: Verify imports resolve**

```bash
python -c "from markets_core.interfaces.classifier import Classifier; print(Classifier)"
python -c "from markets_core.tests.contract.test_classifier_contract import ClassifierContract" 2>&1 | head -5
```

The first must print the `Classifier` protocol class. The second may fail because `tests` is not on the install path — this is fine and normal (contract suites are imported by test files in implementation packages via `pytest` collection, not as a package).

- [ ] **Step 5: Commit**

```bash
git add packages/markets-core/src/markets_core/interfaces/classifier.py \
        packages/markets-core/src/markets_core/interfaces/__init__.py \
        packages/markets-core/tests/contract/__init__.py \
        packages/markets-core/tests/contract/test_classifier_contract.py
git commit -m "feat(core): add Classifier protocol and contract test suite"
```

---

## Task 4: Alembic migration 002 (additive schema changes)

**Files:**
- Create: `infrastructure/sql/migrations/versions/002_stage4_classifier.py`

Existing state from migration 001:
- `derived.zscores` already exists (correct shape).
- `regimes.regime_states` already exists without `rationale_detail` or `ontology_version`.
- `ontology.regime_triggers.condition` is currently `text`.

Migration 002 adds `derived.yoy_changes`, `derived.trends`, alters `regime_triggers.condition` to `jsonb`, and adds two columns to `regime_states`.

- [ ] **Step 1: Create the migration**

Create `infrastructure/sql/migrations/versions/002_stage4_classifier.py`:

```python
"""Stage 4 classifier schema additions

Revision ID: 002
Revises: 001
Create Date: 2026-04-22 00:00:00.000000

"""
from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql


revision = "002"
down_revision = "001"
branch_labels = None
depends_on = None


def upgrade() -> None:
    # derived.yoy_changes
    op.create_table(
        "yoy_changes",
        sa.Column("series_id", sa.Text(), nullable=False),
        sa.Column("observation_date", sa.Date(), nullable=False),
        sa.Column("as_of_date", sa.Date(), nullable=False),
        sa.Column("yoy_pct", sa.Numeric(10, 6), nullable=True),
        sa.Column(
            "computed_at",
            sa.TIMESTAMP(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.PrimaryKeyConstraint("series_id", "observation_date", "as_of_date"),
        schema="derived",
    )
    op.execute("SELECT create_hypertable('derived.yoy_changes', 'observation_date')")

    # derived.trends
    op.create_table(
        "trends",
        sa.Column("series_id", sa.Text(), nullable=False),
        sa.Column("observation_date", sa.Date(), nullable=False),
        sa.Column("as_of_date", sa.Date(), nullable=False),
        sa.Column("window_days", sa.Integer(), nullable=False),
        sa.Column("slope", sa.Numeric(10, 6), nullable=True),
        sa.Column(
            "computed_at",
            sa.TIMESTAMP(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.PrimaryKeyConstraint(
            "series_id", "observation_date", "as_of_date", "window_days"
        ),
        schema="derived",
    )
    op.execute("SELECT create_hypertable('derived.trends', 'observation_date')")

    # ontology.regime_triggers.condition: text -> jsonb
    op.execute(
        "ALTER TABLE ontology.regime_triggers "
        "ALTER COLUMN condition TYPE jsonb USING condition::jsonb"
    )

    # regimes.regime_states: add rationale_detail, ontology_version
    op.add_column(
        "regime_states",
        sa.Column(
            "rationale_detail",
            postgresql.JSONB(),
            nullable=False,
            server_default=sa.text("'{}'::jsonb"),
        ),
        schema="regimes",
    )
    op.add_column(
        "regime_states",
        sa.Column(
            "ontology_version",
            sa.Text(),
            nullable=False,
            server_default=sa.text("''"),
        ),
        schema="regimes",
    )


def downgrade() -> None:
    op.drop_column("regime_states", "ontology_version", schema="regimes")
    op.drop_column("regime_states", "rationale_detail", schema="regimes")
    op.execute(
        "ALTER TABLE ontology.regime_triggers "
        "ALTER COLUMN condition TYPE text USING condition::text"
    )
    op.drop_table("trends", schema="derived")
    op.drop_table("yoy_changes", schema="derived")
```

- [ ] **Step 2: Apply the migration to a clean dev DB**

```bash
make down && make up
# wait for postgres to become healthy (~10s)
make migrate
```

Expected: migration applies with no errors. If `make migrate` is not defined, run `alembic -c alembic.ini upgrade head`.

- [ ] **Step 3: Verify the schema**

```bash
docker compose -f infrastructure/docker/docker-compose.yml exec -T timescaledb \
  psql -U postgres -d markets -c "\d+ regimes.regime_states" -c "\d+ derived.yoy_changes" -c "\d+ derived.trends"
```

Expected output confirms:
- `regimes.regime_states` has `rationale_detail jsonb NOT NULL` and `ontology_version text NOT NULL`.
- `derived.yoy_changes` and `derived.trends` exist with the documented columns.
- `ontology.regime_triggers.condition` is `jsonb` (check via `\d+ ontology.regime_triggers`).

- [ ] **Step 4: Verify downgrade is clean**

```bash
alembic -c alembic.ini downgrade 001
alembic -c alembic.ini upgrade head
```

Both commands must succeed.

- [ ] **Step 5: Commit**

```bash
git add infrastructure/sql/migrations/versions/002_stage4_classifier.py
git commit -m "feat(db): add migration 002 for Stage 4 schema (yoy_changes, trends, rationale_detail, ontology_version, jsonb condition)"
```

---

## Task 5: Warehouse methods for transforms + classifier

**Files:**
- Modify: `packages/markets-pipeline/src/markets_pipeline/stores/timescale.py`
- Test: `packages/markets-pipeline/tests/integration/test_warehouse_methods.py`

These SQL methods are the only place transforms and the classifier touch raw SQL. Read the existing `timescale.py` to see what is already there; add the missing methods.

**Methods to add** (if any are already present, keep them; do not duplicate):

- `read_observations_point_in_time(series_ids: list[str], as_of_date: date, start: date | None = None) -> list[dict]`
  Returns rows with `(series_id, observation_date, as_of_date, value)` using the canonical `DISTINCT ON` pattern.
- `upsert_zscores(rows: list[dict]) -> None` — one row per `(series_id, observation_date, as_of_date, window_days)`.
- `upsert_yoy_changes(rows: list[dict]) -> None`
- `upsert_trends(rows: list[dict]) -> None`
- `read_zscore_point_in_time(variable_series_id: str, window_days: int, as_of_date: date) -> Decimal | None`
- `read_yoy_change_point_in_time(variable_series_id: str, as_of_date: date) -> Decimal | None`
- `read_trend_point_in_time(variable_series_id: str, window_days: int, as_of_date: date) -> Decimal | None`
- `read_raw_value_point_in_time(variable_series_id: str, as_of_date: date) -> Decimal | None`
- `read_ontology_variables() -> list[dict]` — returns `id`, `name`, `primary_series`.
- `read_ontology_regimes() -> list[dict]`
- `read_ontology_regime_triggers() -> list[dict]` — returns with `condition` already deserialized from jsonb.
- `read_ontology_version() -> str` — returns the current Alembic revision.
- `upsert_regime_state(row: dict) -> None`

All "read" methods return `None` (or empty list) if no row satisfies the point-in-time predicate. They do NOT raise — raising is the classifier's job.

- [ ] **Step 1: Write integration test**

Create `packages/markets-pipeline/tests/integration/test_warehouse_methods.py`:

```python
"""Integration tests for warehouse point-in-time reads and derived upserts.

Requires a running Timescale instance (make up; make migrate).
"""
from datetime import date
from decimal import Decimal
import os

import pytest

from markets_pipeline.stores.timescale import TimescaleWarehouse


pytestmark = pytest.mark.integration


@pytest.fixture
def warehouse() -> TimescaleWarehouse:
    dsn = os.environ.get("DATABASE_URL") or "postgresql://postgres:your_password_here@localhost:5432/markets"
    return TimescaleWarehouse(dsn=dsn)


@pytest.fixture
def seeded_observations(warehouse):
    """Insert a few observations for a synthetic series, clean up after."""
    series = "test:INTEGRATION_SERIES"
    rows = [
        {"series_id": series, "observation_date": date(2026, 3, 1),
         "as_of_date": date(2026, 3, 5), "value": Decimal("1.00"),
         "source_revision": None},
        {"series_id": series, "observation_date": date(2026, 3, 1),
         "as_of_date": date(2026, 3, 20), "value": Decimal("1.10"),
         "source_revision": "r1"},
    ]
    warehouse.upsert_observations(rows)
    yield series
    warehouse.execute_sql(
        "DELETE FROM observations.observations WHERE series_id = %s", (series,)
    )


def test_point_in_time_read_returns_latest_known_as_of(warehouse, seeded_observations):
    series = seeded_observations
    result = warehouse.read_observations_point_in_time(
        series_ids=[series], as_of_date=date(2026, 3, 10)
    )
    assert len(result) == 1
    assert result[0]["value"] == Decimal("1.00")  # earlier vintage

    result2 = warehouse.read_observations_point_in_time(
        series_ids=[series], as_of_date=date(2026, 3, 25)
    )
    assert result2[0]["value"] == Decimal("1.10")  # later revision


def test_read_zscore_returns_none_when_missing(warehouse):
    result = warehouse.read_zscore_point_in_time(
        variable_series_id="test:DEFINITELY_NOT_REAL",
        window_days=63,
        as_of_date=date(2026, 4, 15),
    )
    assert result is None


def test_upsert_and_read_zscore_round_trip(warehouse):
    rows = [{
        "series_id": "test:ROUND_TRIP",
        "observation_date": date(2026, 4, 15),
        "as_of_date": date(2026, 4, 15),
        "window_days": 63,
        "zscore": Decimal("1.2345"),
    }]
    warehouse.upsert_zscores(rows)
    try:
        result = warehouse.read_zscore_point_in_time(
            "test:ROUND_TRIP", window_days=63, as_of_date=date(2026, 4, 15)
        )
        assert result == Decimal("1.2345")
    finally:
        warehouse.execute_sql(
            "DELETE FROM derived.zscores WHERE series_id = %s", ("test:ROUND_TRIP",)
        )


def test_read_ontology_version_matches_alembic_head(warehouse):
    v = warehouse.read_ontology_version()
    assert v == "002"
```

- [ ] **Step 2: Run the test to confirm it fails**

```bash
pytest packages/markets-pipeline/tests/integration/test_warehouse_methods.py -v -m integration
```

Expected: fails on missing methods.

- [ ] **Step 3: Add the methods to `timescale.py`**

In `packages/markets-pipeline/src/markets_pipeline/stores/timescale.py`, add the methods below. If the file already has `execute_sql` or `upsert_observations`, do not redefine them — add only the missing methods. Use psycopg v3 patterns consistent with existing code in the file:

```python
from datetime import date
from decimal import Decimal
from typing import Any


# ---- point-in-time observations ---------------------------------------------

def read_observations_point_in_time(
    self, series_ids: list[str], as_of_date: date, start: date | None = None
) -> list[dict[str, Any]]:
    """Return one row per (series_id, observation_date) with latest as_of_date <= as_of_date."""
    sql = """
        SELECT DISTINCT ON (series_id, observation_date)
            series_id, observation_date, as_of_date, value
        FROM observations.observations
        WHERE as_of_date <= %(as_of)s
          AND series_id = ANY(%(series_ids)s)
          {start_filter}
        ORDER BY series_id, observation_date, as_of_date DESC
    """
    start_filter = "AND observation_date >= %(start)s" if start else ""
    sql = sql.format(start_filter=start_filter)
    params: dict[str, Any] = {"as_of": as_of_date, "series_ids": list(series_ids)}
    if start:
        params["start"] = start
    with self._conn() as conn, conn.cursor() as cur:
        cur.execute(sql, params)
        cols = [c.name for c in cur.description]
        return [dict(zip(cols, row)) for row in cur.fetchall()]


# ---- derived upserts --------------------------------------------------------

def upsert_zscores(self, rows: list[dict[str, Any]]) -> None:
    if not rows:
        return
    sql = """
        INSERT INTO derived.zscores
            (series_id, observation_date, as_of_date, window_days, zscore)
        VALUES (%(series_id)s, %(observation_date)s, %(as_of_date)s, %(window_days)s, %(zscore)s)
        ON CONFLICT (series_id, observation_date, as_of_date, window_days)
        DO UPDATE SET zscore = EXCLUDED.zscore, computed_at = now()
    """
    with self._conn() as conn, conn.cursor() as cur:
        cur.executemany(sql, rows)
        conn.commit()


def upsert_yoy_changes(self, rows: list[dict[str, Any]]) -> None:
    if not rows:
        return
    sql = """
        INSERT INTO derived.yoy_changes
            (series_id, observation_date, as_of_date, yoy_pct)
        VALUES (%(series_id)s, %(observation_date)s, %(as_of_date)s, %(yoy_pct)s)
        ON CONFLICT (series_id, observation_date, as_of_date)
        DO UPDATE SET yoy_pct = EXCLUDED.yoy_pct, computed_at = now()
    """
    with self._conn() as conn, conn.cursor() as cur:
        cur.executemany(sql, rows)
        conn.commit()


def upsert_trends(self, rows: list[dict[str, Any]]) -> None:
    if not rows:
        return
    sql = """
        INSERT INTO derived.trends
            (series_id, observation_date, as_of_date, window_days, slope)
        VALUES (%(series_id)s, %(observation_date)s, %(as_of_date)s, %(window_days)s, %(slope)s)
        ON CONFLICT (series_id, observation_date, as_of_date, window_days)
        DO UPDATE SET slope = EXCLUDED.slope, computed_at = now()
    """
    with self._conn() as conn, conn.cursor() as cur:
        cur.executemany(sql, rows)
        conn.commit()


# ---- derived point-in-time reads --------------------------------------------

def _read_latest_scalar(
    self, sql: str, params: dict[str, Any], col: str
) -> Decimal | None:
    with self._conn() as conn, conn.cursor() as cur:
        cur.execute(sql, params)
        row = cur.fetchone()
        return row[0] if row and row[0] is not None else None


def read_zscore_point_in_time(
    self, variable_series_id: str, window_days: int, as_of_date: date
) -> Decimal | None:
    sql = """
        SELECT zscore FROM derived.zscores
        WHERE series_id = %(sid)s
          AND window_days = %(w)s
          AND as_of_date <= %(as_of)s
          AND observation_date <= %(as_of)s
        ORDER BY observation_date DESC, as_of_date DESC
        LIMIT 1
    """
    return self._read_latest_scalar(
        sql, {"sid": variable_series_id, "w": window_days, "as_of": as_of_date}, "zscore"
    )


def read_yoy_change_point_in_time(
    self, variable_series_id: str, as_of_date: date
) -> Decimal | None:
    sql = """
        SELECT yoy_pct FROM derived.yoy_changes
        WHERE series_id = %(sid)s
          AND as_of_date <= %(as_of)s
          AND observation_date <= %(as_of)s
        ORDER BY observation_date DESC, as_of_date DESC
        LIMIT 1
    """
    return self._read_latest_scalar(
        sql, {"sid": variable_series_id, "as_of": as_of_date}, "yoy_pct"
    )


def read_trend_point_in_time(
    self, variable_series_id: str, window_days: int, as_of_date: date
) -> Decimal | None:
    sql = """
        SELECT slope FROM derived.trends
        WHERE series_id = %(sid)s
          AND window_days = %(w)s
          AND as_of_date <= %(as_of)s
          AND observation_date <= %(as_of)s
        ORDER BY observation_date DESC, as_of_date DESC
        LIMIT 1
    """
    return self._read_latest_scalar(
        sql, {"sid": variable_series_id, "w": window_days, "as_of": as_of_date}, "slope"
    )


def read_raw_value_point_in_time(
    self, variable_series_id: str, as_of_date: date
) -> Decimal | None:
    sql = """
        SELECT DISTINCT ON (series_id, observation_date) value
        FROM observations.observations
        WHERE series_id = %(sid)s
          AND as_of_date <= %(as_of)s
          AND observation_date <= %(as_of)s
        ORDER BY series_id, observation_date DESC, as_of_date DESC
        LIMIT 1
    """
    return self._read_latest_scalar(
        sql, {"sid": variable_series_id, "as_of": as_of_date}, "value"
    )


# ---- ontology reads ---------------------------------------------------------

def read_ontology_variables(self) -> list[dict[str, Any]]:
    sql = "SELECT id, name, primary_series, tier FROM ontology.variables"
    with self._conn() as conn, conn.cursor() as cur:
        cur.execute(sql)
        cols = [c.name for c in cur.description]
        return [dict(zip(cols, row)) for row in cur.fetchall()]


def read_ontology_regimes(self) -> list[dict[str, Any]]:
    sql = "SELECT id, name, display_name, description, tier FROM ontology.regimes"
    with self._conn() as conn, conn.cursor() as cur:
        cur.execute(sql)
        cols = [c.name for c in cur.description]
        return [dict(zip(cols, row)) for row in cur.fetchall()]


def read_ontology_regime_triggers(self) -> list[dict[str, Any]]:
    """Returns rows with `condition` deserialized from jsonb into a Python dict."""
    sql = """
        SELECT id, regime_id, variable_id, condition, weight, description
        FROM ontology.regime_triggers
    """
    with self._conn() as conn, conn.cursor() as cur:
        cur.execute(sql)
        cols = [c.name for c in cur.description]
        return [dict(zip(cols, row)) for row in cur.fetchall()]


def read_ontology_version(self) -> str:
    sql = "SELECT version_num FROM alembic_version LIMIT 1"
    with self._conn() as conn, conn.cursor() as cur:
        cur.execute(sql)
        row = cur.fetchone()
        return row[0] if row else ""


# ---- regime states ----------------------------------------------------------

def upsert_regime_state(self, row: dict[str, Any]) -> None:
    sql = """
        INSERT INTO regimes.regime_states
            (as_of_date, classifier_name, classifier_version, regime_name,
             confidence, trigger_variables, rationale, rationale_detail, ontology_version)
        VALUES
            (%(as_of_date)s, %(classifier_name)s, %(classifier_version)s, %(regime_name)s,
             %(confidence)s, %(trigger_variables)s, %(rationale)s,
             %(rationale_detail)s, %(ontology_version)s)
        ON CONFLICT (as_of_date, classifier_name, classifier_version) DO UPDATE SET
            regime_name = EXCLUDED.regime_name,
            confidence = EXCLUDED.confidence,
            trigger_variables = EXCLUDED.trigger_variables,
            rationale = EXCLUDED.rationale,
            rationale_detail = EXCLUDED.rationale_detail,
            ontology_version = EXCLUDED.ontology_version,
            computed_at = now()
    """
    with self._conn() as conn, conn.cursor() as cur:
        # psycopg v3 serializes Python dict to jsonb automatically via Json adapter
        from psycopg.types.json import Json
        payload = dict(row)
        payload["rationale_detail"] = Json(row["rationale_detail"])
        cur.execute(sql, payload)
        conn.commit()


def execute_sql(self, sql: str, params: tuple | None = None) -> None:
    """Escape hatch used by tests to clean up rows."""
    with self._conn() as conn, conn.cursor() as cur:
        cur.execute(sql, params or ())
        conn.commit()
```

If `_conn()` is not the existing method name for opening a connection, read the current class and adapt. If `upsert_observations` is missing, add an analogous method:

```python
def upsert_observations(self, rows: list[dict[str, Any]]) -> None:
    if not rows:
        return
    sql = """
        INSERT INTO observations.observations
            (series_id, observation_date, as_of_date, value, source_revision)
        VALUES (%(series_id)s, %(observation_date)s, %(as_of_date)s, %(value)s, %(source_revision)s)
        ON CONFLICT (series_id, observation_date, as_of_date) DO UPDATE SET
            value = EXCLUDED.value, source_revision = EXCLUDED.source_revision
    """
    with self._conn() as conn, conn.cursor() as cur:
        cur.executemany(sql, rows)
        conn.commit()
```

- [ ] **Step 4: Run the test**

```bash
make up && make migrate   # ensure DB is ready with schema 002
pytest packages/markets-pipeline/tests/integration/test_warehouse_methods.py -v -m integration
```

Expected: all PASS.

- [ ] **Step 5: Commit**

```bash
git add packages/markets-pipeline/src/markets_pipeline/stores/timescale.py \
        packages/markets-pipeline/tests/integration/test_warehouse_methods.py
git commit -m "feat(stores): add point-in-time reads and upserts for stage 4"
```

---

## Task 6: Condition evaluator (pure function)

**Files:**
- Create: `packages/markets-pipeline/src/markets_pipeline/classifiers/__init__.py` (empty)
- Create: `packages/markets-pipeline/src/markets_pipeline/classifiers/conditions.py`
- Test: `packages/markets-pipeline/tests/unit/test_conditions.py`

The evaluator is a pure function — takes a validated `ConditionNode` and a `dict[str, Decimal]` feature row, returns `bool`. It raises `InsufficientDataError` if any required feature value is missing (None) or absent from the feature dict.

The feature-dict key format is `"{metric}:{variable}:{window_days}"` (e.g., `"zscore:brent_oil:63"`, `"raw_value:us_real_10y:"`).

- [ ] **Step 1: Write the failing test**

Create `packages/markets-pipeline/src/markets_pipeline/classifiers/__init__.py` (empty file).

Create `packages/markets-pipeline/tests/unit/test_conditions.py`:

```python
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
        evaluate(node, {"zscore:a:63": Decimal("1.0")})  # b missing


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
```

- [ ] **Step 2: Run test to verify it fails**

```bash
pytest packages/markets-pipeline/tests/unit/test_conditions.py -v
```

Expected: ImportError on `markets_pipeline.classifiers.conditions`.

- [ ] **Step 3: Implement the evaluator**

Create `packages/markets-pipeline/src/markets_pipeline/classifiers/conditions.py`:

```python
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
```

- [ ] **Step 4: Run tests**

```bash
pytest packages/markets-pipeline/tests/unit/test_conditions.py -v
```

Expected: all PASS.

- [ ] **Step 5: Commit**

```bash
git add packages/markets-pipeline/src/markets_pipeline/classifiers/__init__.py \
        packages/markets-pipeline/src/markets_pipeline/classifiers/conditions.py \
        packages/markets-pipeline/tests/unit/test_conditions.py
git commit -m "feat(classifiers): pure-function JSON-AST condition evaluator"
```

---

## Task 7: Three Transform implementations (z-score, YoY, trend)

**Files:**
- Create: `packages/markets-pipeline/src/markets_pipeline/transforms/__init__.py` (empty; create if missing)
- Create: `packages/markets-pipeline/src/markets_pipeline/transforms/zscore.py`
- Create: `packages/markets-pipeline/src/markets_pipeline/transforms/yoy_change.py`
- Create: `packages/markets-pipeline/src/markets_pipeline/transforms/trend.py`
- Test: `packages/markets-pipeline/tests/contract/test_transforms_contract.py`
- Test: `packages/markets-pipeline/tests/unit/test_transforms_math.py`

Each transform implements the existing `Transform` protocol:

```python
class Transform(Protocol):
    def compute(self, as_of_date: date) -> None: ...
```

Internally each transform:
1. Reads the list of variables it needs to process (from `ontology.variables.primary_series`).
2. For each series, reads point-in-time observations up to `as_of_date`.
3. Forward-fills to daily if the native frequency is monthly (detected by sparse gaps, not a flag).
4. Computes the metric for `observation_date = as_of_date` only (one row per run).
5. Upserts into its derived table with `as_of_date = as_of_date`.

"Compute for `observation_date = as_of_date` only" is a deliberate choice: the DAG runs once per logical date; a backfill re-runs the DAG across historical logical dates. This keeps transforms simple and idempotent.

- [ ] **Step 1: Write math unit tests**

Create `packages/markets-pipeline/tests/unit/test_transforms_math.py`:

```python
"""Pure-math unit tests for transforms.

These test the numeric core of each transform in isolation from the DB.
The functions under test are module-level helpers exposed by each transform.
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
    # Zero stddev -> undefined; return None
    assert compute_zscore(daily, window_days=63) is None


def test_zscore_known_values():
    # Linear 1..100; the last value is far above the 63-day window mean
    daily = _series(list(range(1, 101)))
    z = compute_zscore(daily, window_days=63)
    assert z is not None
    # window = last 63 values: 38..100. mean = 69, std ~ 18.19. z = (100-69)/18.19 ~= 1.70
    assert Decimal("1.5") < z < Decimal("2.0")


def test_zscore_insufficient_history_returns_none():
    daily = _series([1.0] * 10)
    assert compute_zscore(daily, window_days=63) is None


def test_yoy_pct_basic():
    # Value today = 110, value 365d ago = 100 -> 0.10
    end = date(2026, 4, 15)
    values = {(end - timedelta(days=365)): Decimal("100"), end: Decimal("110")}
    y = compute_yoy_pct(values, end)
    assert y == Decimal("0.10")


def test_yoy_pct_missing_one_year_ago_returns_none():
    end = date(2026, 4, 15)
    values = {end: Decimal("110")}
    assert compute_yoy_pct(values, end) is None


def test_yoy_pct_tolerates_weekend_offset():
    # If exact 365d-ago is missing, accept the nearest prior obs within 7 days
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
```

- [ ] **Step 2: Run the test to verify it fails**

```bash
pytest packages/markets-pipeline/tests/unit/test_transforms_math.py -v
```

Expected: ImportErrors.

- [ ] **Step 3: Implement the transforms**

Create `packages/markets-pipeline/src/markets_pipeline/transforms/__init__.py` (empty).

Create `packages/markets-pipeline/src/markets_pipeline/transforms/zscore.py`:

```python
"""Z-score transform: computes rolling z-score of a series' value.

At each run we compute z-score for observation_date = as_of_date only, using
the point-in-time history of the series (latest known vintage per observation
date, all with as_of_date <= as_of_date).
"""
from __future__ import annotations
from datetime import date, timedelta
from decimal import Decimal

from markets_pipeline.stores.timescale import TimescaleWarehouse


# Z-score windows the classifier uses
DEFAULT_WINDOWS = (63, 252)


def compute_zscore(
    daily: list[tuple[date, Decimal]], window_days: int
) -> Decimal | None:
    """Return z-score of the latest value relative to the trailing window.

    `daily` must be sorted ascending by date and already forward-filled.
    Returns None if there are fewer than window_days observations or stddev is 0.
    """
    if len(daily) < window_days:
        return None
    window = daily[-window_days:]
    values = [float(v) for _, v in window]
    n = len(values)
    mean = sum(values) / n
    var = sum((x - mean) ** 2 for x in values) / n
    std = var ** 0.5
    if std == 0:
        return None
    latest = float(window[-1][1])
    z = (latest - mean) / std
    return Decimal(str(round(z, 4)))


def _forward_fill_to_daily(
    observations: list[dict], end: date
) -> list[tuple[date, Decimal]]:
    """Turn sparse observations into a dense daily series ending at `end`.

    observations: list of {"observation_date": date, "value": Decimal|None}
    sorted ascending. Gaps are forward-filled using the last known value.
    Days before the first observation are omitted.
    """
    if not observations:
        return []
    observations = sorted(observations, key=lambda r: r["observation_date"])
    out: list[tuple[date, Decimal]] = []
    obs_iter = iter(observations)
    current = next(obs_iter)
    next_obs = next(obs_iter, None)
    d = current["observation_date"]
    last_value = current["value"]
    while d <= end:
        while next_obs is not None and next_obs["observation_date"] <= d:
            if next_obs["value"] is not None:
                last_value = next_obs["value"]
            current = next_obs
            next_obs = next(obs_iter, None)
        if last_value is not None:
            out.append((d, Decimal(last_value)))
        d = d + timedelta(days=1)
    return out


class ZScoreTransform:
    """Transform implementation. Writes derived.zscores."""

    def __init__(self, warehouse: TimescaleWarehouse, windows: tuple[int, ...] = DEFAULT_WINDOWS):
        self.warehouse = warehouse
        self.windows = windows

    def compute(self, as_of_date: date) -> None:
        variables = self.warehouse.read_ontology_variables()
        series_ids = [v["primary_series"] for v in variables if v.get("primary_series")]
        if not series_ids:
            return
        rows_by_series = self._read_history(series_ids, as_of_date)
        out_rows: list[dict] = []
        for series_id, obs in rows_by_series.items():
            daily = _forward_fill_to_daily(obs, as_of_date)
            for w in self.windows:
                z = compute_zscore(daily, w)
                if z is None:
                    continue
                out_rows.append({
                    "series_id": series_id,
                    "observation_date": as_of_date,
                    "as_of_date": as_of_date,
                    "window_days": w,
                    "zscore": z,
                })
        self.warehouse.upsert_zscores(out_rows)

    def _read_history(self, series_ids: list[str], as_of_date: date) -> dict[str, list[dict]]:
        # Use warehouse helper; return grouped by series_id
        # We need full history of each series at/before as_of_date (largest window = 252d
        # of business days ~ 365 calendar days; fetch 400 days for buffer).
        start = as_of_date - timedelta(days=400)
        rows = self.warehouse.read_observations_point_in_time(
            series_ids=series_ids, as_of_date=as_of_date, start=start
        )
        grouped: dict[str, list[dict]] = {sid: [] for sid in series_ids}
        for r in rows:
            grouped[r["series_id"]].append(
                {"observation_date": r["observation_date"], "value": r["value"]}
            )
        return grouped
```

Create `packages/markets-pipeline/src/markets_pipeline/transforms/yoy_change.py`:

```python
"""Year-over-year percent change transform."""
from __future__ import annotations
from datetime import date, timedelta
from decimal import Decimal

from markets_pipeline.stores.timescale import TimescaleWarehouse


def compute_yoy_pct(
    values: dict[date, Decimal], target: date, max_offset_days: int = 7
) -> Decimal | None:
    """Return (value[target] - value[target-365d]) / value[target-365d].

    If the exact 365-day-prior date is absent, search backward up to
    max_offset_days for the nearest prior observation.
    Returns None if target value or any eligible prior value is missing.
    """
    if target not in values or values[target] is None:
        return None
    current = values[target]
    ideal = target - timedelta(days=365)
    for offset in range(0, max_offset_days + 1):
        d = ideal - timedelta(days=offset)
        prior = values.get(d)
        if prior is not None and prior != Decimal(0):
            return (current - prior) / prior
    return None


class YoYChangeTransform:
    def __init__(self, warehouse: TimescaleWarehouse):
        self.warehouse = warehouse

    def compute(self, as_of_date: date) -> None:
        variables = self.warehouse.read_ontology_variables()
        series_ids = [v["primary_series"] for v in variables if v.get("primary_series")]
        if not series_ids:
            return
        start = as_of_date - timedelta(days=400)
        rows = self.warehouse.read_observations_point_in_time(
            series_ids=series_ids, as_of_date=as_of_date, start=start
        )
        per_series: dict[str, dict[date, Decimal]] = {sid: {} for sid in series_ids}
        for r in rows:
            if r["value"] is not None:
                per_series[r["series_id"]][r["observation_date"]] = Decimal(r["value"])
        out_rows: list[dict] = []
        for sid, m in per_series.items():
            y = compute_yoy_pct(m, as_of_date)
            if y is None:
                continue
            out_rows.append({
                "series_id": sid,
                "observation_date": as_of_date,
                "as_of_date": as_of_date,
                "yoy_pct": y.quantize(Decimal("0.000001")),
            })
        self.warehouse.upsert_yoy_changes(out_rows)
```

Create `packages/markets-pipeline/src/markets_pipeline/transforms/trend.py`:

```python
"""Trend transform: OLS slope of values vs. day index over a trailing window."""
from __future__ import annotations
from datetime import date, timedelta
from decimal import Decimal

from markets_pipeline.stores.timescale import TimescaleWarehouse
from markets_pipeline.transforms.zscore import _forward_fill_to_daily


DEFAULT_WINDOWS = (63, 252)


def compute_trend_slope(
    daily: list[tuple[date, Decimal]], window_days: int
) -> Decimal | None:
    """Return OLS slope of value vs. day index over the trailing window.

    `daily` must be sorted ascending and forward-filled.
    Returns None if fewer than window_days rows.
    """
    if len(daily) < window_days:
        return None
    window = daily[-window_days:]
    ys = [float(v) for _, v in window]
    n = len(ys)
    xs = list(range(n))
    x_mean = sum(xs) / n
    y_mean = sum(ys) / n
    num = sum((xs[i] - x_mean) * (ys[i] - y_mean) for i in range(n))
    den = sum((xs[i] - x_mean) ** 2 for i in range(n))
    if den == 0:
        return Decimal("0")
    slope = num / den
    return Decimal(str(round(slope, 6)))


class TrendTransform:
    def __init__(self, warehouse: TimescaleWarehouse, windows: tuple[int, ...] = DEFAULT_WINDOWS):
        self.warehouse = warehouse
        self.windows = windows

    def compute(self, as_of_date: date) -> None:
        variables = self.warehouse.read_ontology_variables()
        series_ids = [v["primary_series"] for v in variables if v.get("primary_series")]
        if not series_ids:
            return
        start = as_of_date - timedelta(days=400)
        rows = self.warehouse.read_observations_point_in_time(
            series_ids=series_ids, as_of_date=as_of_date, start=start
        )
        grouped: dict[str, list[dict]] = {sid: [] for sid in series_ids}
        for r in rows:
            grouped[r["series_id"]].append(
                {"observation_date": r["observation_date"], "value": r["value"]}
            )
        out_rows: list[dict] = []
        for sid, obs in grouped.items():
            daily = _forward_fill_to_daily(obs, as_of_date)
            for w in self.windows:
                slope = compute_trend_slope(daily, w)
                if slope is None:
                    continue
                out_rows.append({
                    "series_id": sid,
                    "observation_date": as_of_date,
                    "as_of_date": as_of_date,
                    "window_days": w,
                    "slope": slope,
                })
        self.warehouse.upsert_trends(out_rows)
```

- [ ] **Step 4: Run math unit tests**

```bash
pytest packages/markets-pipeline/tests/unit/test_transforms_math.py -v
```

Expected: all PASS.

- [ ] **Step 5: Write transform contract subclasses**

Create `packages/markets-pipeline/tests/contract/test_transforms_contract.py`:

```python
"""Contract tests for the three Stage 4 Transform implementations.

These are integration tests because Transform.compute has real DB side effects.
The core contract per CONVENTIONS.md + TESTING.md is:
- .compute(d) is idempotent for a given as-of date.
- Writes exactly one row per series it processes (per window where applicable).
- Reads are point-in-time: data with as_of_date > d must not influence output.
"""
from datetime import date, timedelta
from decimal import Decimal
import os
import pytest

from markets_pipeline.stores.timescale import TimescaleWarehouse
from markets_pipeline.transforms.zscore import ZScoreTransform
from markets_pipeline.transforms.yoy_change import YoYChangeTransform
from markets_pipeline.transforms.trend import TrendTransform


pytestmark = pytest.mark.integration


@pytest.fixture
def warehouse() -> TimescaleWarehouse:
    dsn = os.environ.get("DATABASE_URL") or "postgresql://postgres:your_password_here@localhost:5432/markets"
    return TimescaleWarehouse(dsn=dsn)


@pytest.fixture
def synthetic_series(warehouse):
    """Insert 300 daily observations for a synthetic test series."""
    sid = "test:TRANSFORM_CONTRACT"
    # Insert a minimal ontology row so the transform finds this series
    warehouse.execute_sql(
        "INSERT INTO ontology.variables (name, display_name, tier, primary_series) "
        "VALUES (%s, %s, %s, %s) ON CONFLICT (name) DO NOTHING",
        ("test_transform_contract", "Test Transform Contract", 2, sid),
    )
    start = date(2025, 6, 1)
    rows = []
    for i in range(320):
        d = start + timedelta(days=i)
        rows.append({
            "series_id": sid,
            "observation_date": d,
            "as_of_date": d,
            "value": Decimal(str(1.0 + i * 0.01)),
            "source_revision": None,
        })
    warehouse.upsert_observations(rows)
    yield sid, start + timedelta(days=300)
    warehouse.execute_sql("DELETE FROM observations.observations WHERE series_id = %s", (sid,))
    warehouse.execute_sql("DELETE FROM derived.zscores WHERE series_id = %s", (sid,))
    warehouse.execute_sql("DELETE FROM derived.yoy_changes WHERE series_id = %s", (sid,))
    warehouse.execute_sql("DELETE FROM derived.trends WHERE series_id = %s", (sid,))
    warehouse.execute_sql("DELETE FROM ontology.variables WHERE name = %s",
                          ("test_transform_contract",))


class TestZScoreTransformContract:
    def test_compute_writes_row(self, warehouse, synthetic_series):
        sid, as_of = synthetic_series
        ZScoreTransform(warehouse).compute(as_of)
        v = warehouse.read_zscore_point_in_time(sid, window_days=63, as_of_date=as_of)
        assert v is not None

    def test_compute_is_idempotent(self, warehouse, synthetic_series):
        sid, as_of = synthetic_series
        t = ZScoreTransform(warehouse)
        t.compute(as_of)
        v1 = warehouse.read_zscore_point_in_time(sid, 63, as_of)
        t.compute(as_of)
        v2 = warehouse.read_zscore_point_in_time(sid, 63, as_of)
        assert v1 == v2


class TestYoYChangeTransformContract:
    def test_compute_writes_row(self, warehouse, synthetic_series):
        sid, as_of = synthetic_series
        YoYChangeTransform(warehouse).compute(as_of)
        v = warehouse.read_yoy_change_point_in_time(sid, as_of)
        assert v is not None

    def test_compute_is_idempotent(self, warehouse, synthetic_series):
        sid, as_of = synthetic_series
        t = YoYChangeTransform(warehouse)
        t.compute(as_of)
        v1 = warehouse.read_yoy_change_point_in_time(sid, as_of)
        t.compute(as_of)
        v2 = warehouse.read_yoy_change_point_in_time(sid, as_of)
        assert v1 == v2


class TestTrendTransformContract:
    def test_compute_writes_row(self, warehouse, synthetic_series):
        sid, as_of = synthetic_series
        TrendTransform(warehouse).compute(as_of)
        v = warehouse.read_trend_point_in_time(sid, window_days=63, as_of_date=as_of)
        assert v is not None

    def test_compute_is_idempotent(self, warehouse, synthetic_series):
        sid, as_of = synthetic_series
        t = TrendTransform(warehouse)
        t.compute(as_of)
        v1 = warehouse.read_trend_point_in_time(sid, 63, as_of)
        t.compute(as_of)
        v2 = warehouse.read_trend_point_in_time(sid, 63, as_of)
        assert v1 == v2
```

- [ ] **Step 6: Run contract tests**

```bash
make up && make migrate
pytest packages/markets-pipeline/tests/contract/test_transforms_contract.py -v -m integration
```

Expected: all PASS.

- [ ] **Step 7: Commit**

```bash
git add packages/markets-pipeline/src/markets_pipeline/transforms/ \
        packages/markets-pipeline/tests/unit/test_transforms_math.py \
        packages/markets-pipeline/tests/contract/test_transforms_contract.py
git commit -m "feat(transforms): z-score, YoY change, trend transforms with point-in-time reads"
```

---

## Task 8: `RuleBasedClassifier` + contract subclass

**Files:**
- Create: `packages/markets-pipeline/src/markets_pipeline/classifiers/rule_based.py`
- Test: `packages/markets-pipeline/tests/unit/test_rule_based_scoring.py`
- Test: `packages/markets-pipeline/tests/contract/test_rule_based_contract.py`

The classifier:
1. At construction: loads regimes and triggers from ontology (parsed via `parse_condition` into `RegimeTrigger` models), reads `ontology_version` from the warehouse, records classifier name/version.
2. At `classify(as_of_date)`:
   a. Determines the set of feature keys needed (union of `required_features()` across all triggers).
   b. For each feature key, reads the point-in-time value from the warehouse. A missing-but-required feature raises `InsufficientDataError`.
   c. Evaluates each trigger; records a `TriggerEvaluation`.
   d. Computes per-regime weighted scores.
   e. Picks winner: highest stress-regime score if any ≥ 0.5 threshold; else `benign_expansion` with confidence `1 - max_stress_score`.
   f. Deterministic tiebreak: highest total weight, then alphabetical.
   g. Populates `rationale` (templated sentence), `rationale_detail` (full dict), `trigger_variables`.

Name: `"rule_based"`. Version: `"1.0.0"`.

The classifier identifies `benign_expansion` by the regime NAME `"benign_expansion"`; other regimes are "stress regimes." This is deliberate — the name is the identifier of the default regime per the spec.

Note: variables and triggers use `variable_id` in the DB, but conditions reference the variable's `name`. The classifier looks up `name → primary_series` via `ontology.variables` at construction.

- [ ] **Step 1: Write scoring unit tests**

Create `packages/markets-pipeline/tests/unit/test_rule_based_scoring.py`:

```python
"""Unit tests for classifier scoring math (no DB)."""
from decimal import Decimal
from markets_pipeline.classifiers.rule_based import (
    weighted_score,
    pick_winner,
)


def test_weighted_score_all_satisfied():
    triggers = [
        {"weight": Decimal("0.4"), "satisfied": True},
        {"weight": Decimal("0.3"), "satisfied": True},
        {"weight": Decimal("0.3"), "satisfied": True},
    ]
    assert weighted_score(triggers) == Decimal("1.0")


def test_weighted_score_partial():
    triggers = [
        {"weight": Decimal("0.4"), "satisfied": True},
        {"weight": Decimal("0.3"), "satisfied": False},
        {"weight": Decimal("0.3"), "satisfied": True},
    ]
    # (0.4 + 0.3) / 1.0 = 0.7
    assert weighted_score(triggers) == Decimal("0.7")


def test_weighted_score_none_satisfied():
    triggers = [
        {"weight": Decimal("0.4"), "satisfied": False},
        {"weight": Decimal("0.3"), "satisfied": False},
    ]
    assert weighted_score(triggers) == Decimal("0")


def test_weighted_score_empty_returns_zero():
    assert weighted_score([]) == Decimal("0")


def test_pick_winner_highest_stress_score():
    scores = {
        "monetary_tightening": Decimal("0.82"),
        "monetary_easing": Decimal("0.10"),
        "supply_shock": Decimal("0.25"),
    }
    weights = {"monetary_tightening": Decimal("1.0"),
               "monetary_easing": Decimal("1.0"),
               "supply_shock": Decimal("1.0")}
    winner, confidence, tiebreak = pick_winner(
        scores, weights, benign_threshold=Decimal("0.5")
    )
    assert winner == "monetary_tightening"
    assert confidence == Decimal("0.82")
    assert tiebreak is False


def test_pick_winner_benign_when_all_below_threshold():
    scores = {
        "monetary_tightening": Decimal("0.30"),
        "supply_shock": Decimal("0.10"),
    }
    weights = {"monetary_tightening": Decimal("1.0"),
               "supply_shock": Decimal("1.0")}
    winner, confidence, tiebreak = pick_winner(
        scores, weights, benign_threshold=Decimal("0.5")
    )
    assert winner == "benign_expansion"
    assert confidence == Decimal("0.70")  # 1 - 0.30
    assert tiebreak is False


def test_pick_winner_tiebreak_by_weight():
    scores = {"monetary_tightening": Decimal("0.8"), "supply_shock": Decimal("0.8")}
    weights = {"monetary_tightening": Decimal("0.6"), "supply_shock": Decimal("1.0")}
    winner, _confidence, tiebreak = pick_winner(
        scores, weights, benign_threshold=Decimal("0.5")
    )
    assert winner == "supply_shock"
    assert tiebreak is True


def test_pick_winner_tiebreak_by_name_when_weight_equal():
    scores = {"monetary_tightening": Decimal("0.8"), "supply_shock": Decimal("0.8")}
    weights = {"monetary_tightening": Decimal("1.0"), "supply_shock": Decimal("1.0")}
    winner, _confidence, tiebreak = pick_winner(
        scores, weights, benign_threshold=Decimal("0.5")
    )
    assert winner == "monetary_tightening"  # alphabetical
    assert tiebreak is True
```

- [ ] **Step 2: Run scoring tests to verify failure**

```bash
pytest packages/markets-pipeline/tests/unit/test_rule_based_scoring.py -v
```

Expected: ImportError.

- [ ] **Step 3: Implement the classifier**

Create `packages/markets-pipeline/src/markets_pipeline/classifiers/rule_based.py`:

```python
"""Rule-based regime classifier."""
from __future__ import annotations
from datetime import date
from decimal import Decimal
from typing import Any

from markets_core.domain.regime import (
    ConditionNode,
    MetricRef,
    RegimeClassification,
    RegimeTrigger,
    TriggerEvaluation,
    parse_condition,
)
from markets_core.errors import ConfigurationError, InsufficientDataError

from markets_pipeline.classifiers.conditions import (
    evaluate,
    feature_key,
    required_features,
)
from markets_pipeline.stores.timescale import TimescaleWarehouse


BENIGN_REGIME_NAME = "benign_expansion"
BENIGN_THRESHOLD = Decimal("0.5")


def weighted_score(triggers: list[dict[str, Any]]) -> Decimal:
    if not triggers:
        return Decimal("0")
    total = sum((t["weight"] for t in triggers), start=Decimal("0"))
    if total == 0:
        return Decimal("0")
    satisfied_weight = sum(
        (t["weight"] for t in triggers if t["satisfied"]), start=Decimal("0")
    )
    return satisfied_weight / total


def pick_winner(
    scores: dict[str, Decimal],
    total_weights: dict[str, Decimal],
    benign_threshold: Decimal,
) -> tuple[str, Decimal, bool]:
    """Return (winner_name, confidence, tiebreak_applied).

    Rule: highest-scoring stress regime if any scores >= threshold;
    else benign_expansion with confidence (1 - max_stress_score).
    """
    if not scores:
        return BENIGN_REGIME_NAME, Decimal("1.0"), False
    max_score = max(scores.values())
    if max_score < benign_threshold:
        return BENIGN_REGIME_NAME, Decimal("1") - max_score, False
    top = [name for name, s in scores.items() if s == max_score]
    tiebreak = len(top) > 1
    if tiebreak:
        # Highest total weight wins; then alphabetical
        top.sort(key=lambda n: (-total_weights.get(n, Decimal("0")), n))
    return top[0], max_score, tiebreak


class RuleBasedClassifier:
    """Classifier implementation. Name 'rule_based', version '1.0.0'."""

    name: str = "rule_based"
    version: str = "1.0.0"

    def __init__(self, warehouse: TimescaleWarehouse):
        self.warehouse = warehouse
        self._load_ontology()

    def _load_ontology(self) -> None:
        self._ontology_version = self.warehouse.read_ontology_version()
        variables = self.warehouse.read_ontology_variables()
        # name -> primary_series
        self._var_name_to_series: dict[str, str] = {}
        self._var_id_to_name: dict[int, str] = {}
        for v in variables:
            if v.get("primary_series"):
                self._var_name_to_series[v["name"]] = v["primary_series"]
            self._var_id_to_name[v["id"]] = v["name"]

        regimes = self.warehouse.read_ontology_regimes()
        self._regime_id_to_name: dict[int, str] = {r["id"]: r["name"] for r in regimes}
        self._stress_regime_names: list[str] = [
            r["name"] for r in regimes if r["name"] != BENIGN_REGIME_NAME
        ]

        raw_triggers = self.warehouse.read_ontology_regime_triggers()
        self._triggers: list[RegimeTrigger] = []
        for t in raw_triggers:
            try:
                cond = parse_condition(t["condition"])
                trig = RegimeTrigger(
                    id=t["id"],
                    regime_id=t["regime_id"],
                    variable_id=t["variable_id"],
                    condition=cond,
                    weight=Decimal(t["weight"]),
                    description=t.get("description"),
                )
            except Exception as e:
                raise ConfigurationError(
                    f"malformed trigger id={t['id']}: {e}"
                ) from e
            self._triggers.append(trig)

        # Validate: every condition variable name has a known primary_series
        for trig in self._triggers:
            for key in required_features(trig.condition):
                _metric, var_name, _w = key.split(":", 2)
                if var_name not in self._var_name_to_series:
                    raise ConfigurationError(
                        f"trigger id={trig.id} references unknown variable '{var_name}'"
                    )

    def _read_features(self, as_of_date: date) -> dict[str, Decimal | None]:
        """Build feature dict by reading each required feature point-in-time."""
        keys: set[str] = set()
        for trig in self._triggers:
            for k in required_features(trig.condition):
                keys.add(k)
        features: dict[str, Decimal | None] = {}
        for k in keys:
            metric, var_name, w = k.split(":", 2)
            series_id = self._var_name_to_series[var_name]
            window = int(w) if w else None
            if metric == "zscore":
                assert window is not None
                features[k] = self.warehouse.read_zscore_point_in_time(
                    series_id, window, as_of_date
                )
            elif metric == "yoy_change":
                features[k] = self.warehouse.read_yoy_change_point_in_time(
                    series_id, as_of_date
                )
            elif metric == "trend":
                assert window is not None
                features[k] = self.warehouse.read_trend_point_in_time(
                    series_id, window, as_of_date
                )
            elif metric == "raw_value":
                features[k] = self.warehouse.read_raw_value_point_in_time(
                    series_id, as_of_date
                )
            else:
                raise ConfigurationError(f"unknown metric '{metric}'")
        return features

    def classify(self, as_of_date: date) -> RegimeClassification:
        features = self._read_features(as_of_date)
        # Per-trigger evaluation. Raises InsufficientDataError if any required feature null.
        evaluations: list[TriggerEvaluation] = []
        per_regime: dict[str, list[dict[str, Any]]] = {
            n: [] for n in self._regime_id_to_name.values()
        }
        for trig in self._triggers:
            regime_name = self._regime_id_to_name[trig.regime_id]
            satisfied = evaluate(trig.condition, features)
            evaluations.append(self._build_evaluation(trig, regime_name, satisfied, features))
            per_regime[regime_name].append(
                {"weight": trig.weight, "satisfied": satisfied}
            )

        # Score only stress regimes (benign has no triggers / no score)
        scores: dict[str, Decimal] = {}
        total_weights: dict[str, Decimal] = {}
        for name in self._stress_regime_names:
            triggers_for_regime = per_regime.get(name, [])
            scores[name] = weighted_score(triggers_for_regime)
            total_weights[name] = sum(
                (t["weight"] for t in triggers_for_regime), start=Decimal("0")
            )

        winner, confidence, tiebreak = pick_winner(scores, total_weights, BENIGN_THRESHOLD)

        # trigger_variables: variable names for satisfied triggers of the winner
        if winner == BENIGN_REGIME_NAME:
            trigger_variables: list[str] = []
        else:
            trigger_variables = sorted({
                self._var_id_to_name[t.variable_id]
                for t in self._triggers
                if self._regime_id_to_name[t.regime_id] == winner
                and any(e.trigger_id == t.id and e.satisfied for e in evaluations)
            })

        rationale = self._build_rationale(winner, confidence, scores, per_regime, tiebreak)
        rationale_detail = {
            "winner": winner,
            "score": str(confidence),
            "regime_scores": {k: str(v) for k, v in scores.items()},
            "triggers": [e.model_dump(mode="json") for e in evaluations],
            "tiebreak_applied": tiebreak,
        }

        return RegimeClassification(
            as_of_date=as_of_date,
            regime_name=winner,
            confidence=confidence.quantize(Decimal("0.001")),
            trigger_variables=trigger_variables,
            rationale=rationale,
            rationale_detail=rationale_detail,
            classifier_name=self.name,
            classifier_version=self.version,
            ontology_version=self._ontology_version,
        )

    def _build_evaluation(
        self,
        trig: RegimeTrigger,
        regime_name: str,
        satisfied: bool,
        features: dict[str, Decimal | None],
    ) -> TriggerEvaluation:
        cond = trig.condition
        # Only compare leaves have metric/variable/etc; for compound nodes we leave those None.
        from markets_core.domain.regime import CompareNode, CompareSpreadNode
        if isinstance(cond, CompareNode):
            val = features.get(feature_key(cond.metric, cond.variable, cond.window_days))
            return TriggerEvaluation(
                regime=regime_name,
                trigger_id=trig.id,
                weight=trig.weight,
                satisfied=satisfied,
                metric=cond.metric,
                variable=cond.variable,
                window_days=cond.window_days,
                value=val,
                op=cond.op,
                threshold=cond.value,
                description=trig.description,
            )
        if isinstance(cond, CompareSpreadNode):
            return TriggerEvaluation(
                regime=regime_name,
                trigger_id=trig.id,
                weight=trig.weight,
                satisfied=satisfied,
                metric=f"spread({cond.left.metric}-{cond.right.metric})",
                variable=f"{cond.left.variable}-{cond.right.variable}",
                window_days=cond.left.window_days,
                value=None,
                op=cond.op,
                threshold=cond.value,
                description=trig.description,
            )
        return TriggerEvaluation(
            regime=regime_name,
            trigger_id=trig.id,
            weight=trig.weight,
            satisfied=satisfied,
            description=trig.description,
        )

    def _build_rationale(
        self,
        winner: str,
        confidence: Decimal,
        scores: dict[str, Decimal],
        per_regime: dict[str, list[dict[str, Any]]],
        tiebreak: bool,
    ) -> str:
        parts = [f"Classified {winner} (confidence {confidence:.2f})."]
        if winner != BENIGN_REGIME_NAME and winner in per_regime:
            total = len(per_regime[winner])
            satisfied = sum(1 for t in per_regime[winner] if t["satisfied"])
            parts.append(f"{satisfied} of {total} triggers satisfied.")
        if tiebreak:
            parts.append("Tiebreak applied.")
        return " ".join(parts)
```

- [ ] **Step 4: Run scoring tests**

```bash
pytest packages/markets-pipeline/tests/unit/test_rule_based_scoring.py -v
```

Expected: all PASS.

- [ ] **Step 5: Write classifier contract subclass**

Create `packages/markets-pipeline/tests/contract/test_rule_based_contract.py`:

```python
"""Integration contract test for RuleBasedClassifier.

Requires Timescale up, migration 002 applied, and the seeded ontology.
Also requires that derived.* tables contain at least enough rows to satisfy
the classifier at `known_good_as_of_date`. See the test_stage4_end_to_end
integration test for the full flow — this contract file relies on fixtures
it sets up.
"""
from datetime import date
import os

import pytest

from markets_core.tests.contract.test_classifier_contract import ClassifierContract  # noqa
from markets_pipeline.classifiers.rule_based import RuleBasedClassifier
from markets_pipeline.stores.timescale import TimescaleWarehouse


pytestmark = pytest.mark.integration


@pytest.fixture
def warehouse() -> TimescaleWarehouse:
    dsn = os.environ.get("DATABASE_URL") or "postgresql://postgres:your_password_here@localhost:5432/markets"
    return TimescaleWarehouse(dsn=dsn)


class TestRuleBasedClassifierContract(ClassifierContract):
    @pytest.fixture
    def classifier(self, warehouse, stage4_seeded_fixtures) -> RuleBasedClassifier:
        return RuleBasedClassifier(warehouse=warehouse)

    @pytest.fixture
    def known_good_as_of_date(self, stage4_seeded_fixtures) -> date:
        # stage4_seeded_fixtures returns the "good" and "insufficient" dates
        return stage4_seeded_fixtures["good_date"]

    @pytest.fixture
    def known_insufficient_as_of_date(self, stage4_seeded_fixtures) -> date:
        return stage4_seeded_fixtures["insufficient_date"]
```

The `stage4_seeded_fixtures` fixture is defined in Task 11 (end-to-end integration test) because it requires the full chain. If you run this contract test before Task 11 exists, the fixture will be unresolved — that's expected.

- [ ] **Step 6: Commit**

```bash
git add packages/markets-pipeline/src/markets_pipeline/classifiers/rule_based.py \
        packages/markets-pipeline/tests/unit/test_rule_based_scoring.py \
        packages/markets-pipeline/tests/contract/test_rule_based_contract.py
git commit -m "feat(classifiers): rule-based classifier with weighted scoring and structured rationale"
```

---

## Task 9: Minimal `YfinanceSource` + contract test

**Files:**
- Create: `packages/markets-pipeline/src/markets_pipeline/sources/yfinance.py`
- Create: `packages/markets-pipeline/tests/contract/test_yfinance_contract.py`
- Modify: `packages/markets-pipeline/pyproject.toml` (add `yfinance` dep)

This source implements the existing `Source` protocol. For daily OHLC it produces **four observations per day per ticker**: `<ticker>:open`, `<ticker>:high`, `<ticker>:low`, `<ticker>:close`. The primary series used by Stage 4 ontology are the `:close` variants (`yfinance:DX-Y.NYB:close`, `yfinance:BZ=F:close`) — update the seed accordingly. This keeps the "no sub-daily data" rule explicit and preserves OHLC.

Wait — the spec's §6.1 writes `primary_series` as `yfinance:DX-Y.NYB` and `yfinance:BZ=F`. We have two clean choices:

1. Native IDs are `DX-Y.NYB:close`, `BZ=F:close`. Primary series strings are `yfinance:DX-Y.NYB:close` and `yfinance:BZ=F:close`. Spec wording becomes "the close variant of these tickers." Adjust seed and spec in this task.
2. Pull close-only (single observation per day) and encode OHLC later.

Go with #1 (OHLC preserved; explicit channel suffix).

- [ ] **Step 1: Add the dependency**

Edit `packages/markets-pipeline/pyproject.toml`. Under the appropriate dependencies section, add `"yfinance>=0.2.40"`. If a dev or optional-deps block is already established, follow that pattern.

- [ ] **Step 2: Write the source**

Create `packages/markets-pipeline/src/markets_pipeline/sources/yfinance.py`:

```python
"""yfinance source. Daily OHLC only; no intraday data.

Native IDs are of the form "<ticker>:<channel>" where channel is
open|high|low|close. Four observations per trading day per ticker.
"""
from __future__ import annotations
from collections.abc import Iterator
from datetime import date, datetime, timezone
from decimal import Decimal
from typing import Any

from markets_core.domain.observation import Observation
from markets_core.domain.series import SeriesId
from markets_core.errors import DataSourceError, SeriesNotFoundError


_SUPPORTED_CHANNELS = ("open", "high", "low", "close")
_SUPPORTED_TICKERS = ("DX-Y.NYB", "BZ=F")


def _split_native(native_id: str) -> tuple[str, str]:
    if ":" not in native_id:
        raise SeriesNotFoundError(
            f"yfinance native_id must be '<ticker>:<channel>', got {native_id!r}"
        )
    ticker, channel = native_id.rsplit(":", 1)
    if ticker not in _SUPPORTED_TICKERS:
        raise SeriesNotFoundError(f"unsupported yfinance ticker: {ticker}")
    if channel not in _SUPPORTED_CHANNELS:
        raise SeriesNotFoundError(f"unsupported yfinance channel: {channel}")
    return ticker, channel


class YfinanceSource:
    source_name: str = "yfinance"

    def __init__(self, ticker_allowlist: tuple[str, ...] = _SUPPORTED_TICKERS):
        self._allowed = ticker_allowlist

    def fetch(
        self,
        series_id: SeriesId,
        start: date,
        end: date,
        as_of: date | None = None,
    ) -> Iterator[Observation]:
        ticker, channel = _split_native(series_id.native_id)
        if ticker not in self._allowed:
            raise SeriesNotFoundError(f"ticker not in allowlist: {ticker}")
        import yfinance as yf  # import here to keep test discovery cheap
        try:
            tkr = yf.Ticker(ticker)
            hist = tkr.history(start=start.isoformat(), end=end.isoformat(),
                               interval="1d", auto_adjust=False)
        except Exception as e:
            raise DataSourceError(f"yfinance fetch failed for {ticker}: {e}") from e
        if hist is None or hist.empty:
            # Empty is not an error — the caller asked for a range with no trading days
            return
        column_map = {"open": "Open", "high": "High", "low": "Low", "close": "Close"}
        col = column_map[channel]
        as_of_dt = as_of or date.today()
        now = datetime.now(timezone.utc)
        for idx, row in hist.iterrows():
            obs_date = idx.date() if hasattr(idx, "date") else idx
            value: Any = row[col]
            if value is None:
                continue
            yield Observation(
                series_id=series_id,
                observation_date=obs_date,
                as_of_date=as_of_dt,
                value=Decimal(str(round(float(value), 8))),
                ingested_at=now,
                source_revision=None,
            )

    def health_check(self) -> dict:
        try:
            import yfinance as yf
            _ = yf.Ticker(_SUPPORTED_TICKERS[0]).info  # best-effort probe
            return {"healthy": True}
        except Exception as e:
            return {"healthy": False, "error": str(e)}

    def metadata(self, series_id: SeriesId) -> dict:
        ticker, channel = _split_native(series_id.native_id)
        return {
            "source": self.source_name,
            "native_id": series_id.native_id,
            "ticker": ticker,
            "channel": channel,
            "frequency": "daily",
        }
```

- [ ] **Step 3: Write the contract test**

The repo has no `Source` contract suite published under `markets-core/tests/contract/` (only `transform` and `classifier`). A minimal per-implementation contract test is acceptable — add the shared `Source` contract in a later PR. For now, write direct tests:

Create `packages/markets-pipeline/tests/contract/test_yfinance_contract.py`:

```python
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
        start=date(2026, 3, 1), end=date(2026, 3, 15),
    )
    assert hasattr(result, "__iter__")


def test_fetch_observations_have_valid_dates():
    src = YfinanceSource()
    obs_list = list(src.fetch(
        SeriesId(source="yfinance", native_id="BZ=F:close"),
        start=date(2026, 3, 1), end=date(2026, 3, 15),
    ))
    assert len(obs_list) > 0
    for obs in obs_list:
        assert obs.observation_date <= obs.as_of_date
        assert obs.observation_date >= date(2026, 3, 1)
        assert obs.observation_date < date(2026, 3, 15)


def test_fetch_rejects_unknown_ticker():
    src = YfinanceSource()
    with pytest.raises(SeriesNotFoundError):
        list(src.fetch(
            SeriesId(source="yfinance", native_id="NOT_A_REAL:close"),
            start=date(2026, 3, 1), end=date(2026, 3, 15),
        ))


def test_fetch_rejects_bad_native_id_format():
    src = YfinanceSource()
    with pytest.raises(SeriesNotFoundError):
        list(src.fetch(
            SeriesId(source="yfinance", native_id="BZ=F"),  # no channel
            start=date(2026, 3, 1), end=date(2026, 3, 15),
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
```

- [ ] **Step 4: Run tests**

```bash
uv pip install yfinance  # if not already installed
pytest packages/markets-pipeline/tests/contract/test_yfinance_contract.py -v -m integration
```

Expected: all PASS (network-dependent).

- [ ] **Step 5: Commit**

```bash
git add packages/markets-pipeline/src/markets_pipeline/sources/yfinance.py \
        packages/markets-pipeline/tests/contract/test_yfinance_contract.py \
        packages/markets-pipeline/pyproject.toml
git commit -m "feat(sources): minimal yfinance source with daily OHLC (close channel used by classifier)"
```

---

## Task 10: Ontology seed + seed loader

**Files:**
- Modify: `infrastructure/sql/seed/ontology.sql`
- Modify: `Makefile` (ensure `seed-ontology` target runs cleanly against the new seed)

Replace the toy seed with the full 7-variable, 4-regime content per spec §6. Triggers reference variables by ID — assume auto-incremented IDs matching insertion order (variables inserted in the order listed get IDs 1-7).

Note: `ontology.regime_triggers.condition` is now `jsonb`, so insert literal JSON via the `'...'::jsonb` cast.

- [ ] **Step 1: Replace the seed file**

Overwrite `infrastructure/sql/seed/ontology.sql`:

```sql
-- Stage 4 ontology seed: 7 variables, 4 regimes, 9 triggers.
-- Idempotent: uses ON CONFLICT to upsert by unique name where possible.
-- regime_triggers has no natural unique key other than (id); this seed
-- assumes a fresh DB — truncate if re-running.

BEGIN;

TRUNCATE TABLE ontology.regime_triggers RESTART IDENTITY CASCADE;
TRUNCATE TABLE ontology.regimes RESTART IDENTITY CASCADE;
TRUNCATE TABLE ontology.variables RESTART IDENTITY CASCADE;

-- Variables (IDs 1..7 after TRUNCATE + RESTART IDENTITY)
INSERT INTO ontology.variables (name, display_name, description, tier, primary_series) VALUES
  ('us_10y_treasury',  'US 10-Year Treasury Yield', 'US 10-year Treasury yield', 2, 'fred:DGS10'),
  ('us_2y_treasury',   'US 2-Year Treasury Yield',  'US 2-year Treasury yield',  2, 'fred:DGS2'),
  ('us_real_10y',      'US 10-Year Real Yield',     'US 10-year TIPS yield',     2, 'fred:DFII10'),
  ('dxy',              'US Dollar Index',           'Broad USD index (DXY)',     2, 'yfinance:DX-Y.NYB:close'),
  ('brent_oil',        'Brent Crude Oil',           'Brent crude futures (BZ=F)',2, 'yfinance:BZ=F:close'),
  ('core_cpi_yoy',     'Core CPI YoY',              'US core CPI all items less food and energy', 2, 'fred:CPILFESL'),
  ('headline_cpi_yoy', 'Headline CPI YoY',          'US CPI all urban consumers',2, 'fred:CPIAUCSL');

-- Regimes (IDs 1..4 after TRUNCATE)
INSERT INTO ontology.regimes (name, display_name, description, tier) VALUES
  ('benign_expansion',    'Benign Expansion',    'Default state; no stress regime triggered',         2),
  ('monetary_tightening', 'Monetary Tightening', 'Rates leading, dollar strengthening, real rates rising', 2),
  ('monetary_easing',     'Monetary Easing',     'Rates falling, dollar weakening, real rates dropping', 2),
  ('supply_shock',        'Supply Shock',        'Energy leading, headline inflation diverging from core', 2);

-- Regime triggers
-- monetary_tightening (regime_id=2)
INSERT INTO ontology.regime_triggers (regime_id, variable_id, condition, weight, description) VALUES
  (2, 3,
   '{"type":"compare","metric":"zscore","variable":"us_real_10y","window_days":63,"op":">","value":"1.0"}'::jsonb,
   0.4, 'Real 10y z-score 3m > 1.0'),
  (2, 4,
   '{"type":"compare","metric":"zscore","variable":"dxy","window_days":63,"op":">","value":"1.0"}'::jsonb,
   0.3, 'DXY z-score 3m > 1.0'),
  (2, 3,
   '{"type":"compare","metric":"raw_value","variable":"us_real_10y","window_days":null,"op":">","value":"0"}'::jsonb,
   0.3, 'Real 10y > 0');

-- monetary_easing (regime_id=3)
INSERT INTO ontology.regime_triggers (regime_id, variable_id, condition, weight, description) VALUES
  (3, 3,
   '{"type":"compare","metric":"zscore","variable":"us_real_10y","window_days":63,"op":"<","value":"-1.0"}'::jsonb,
   0.4, 'Real 10y z-score 3m < -1.0'),
  (3, 4,
   '{"type":"compare","metric":"zscore","variable":"dxy","window_days":63,"op":"<","value":"-1.0"}'::jsonb,
   0.3, 'DXY z-score 3m < -1.0'),
  (3, 1,
   '{"type":"compare","metric":"trend","variable":"us_10y_treasury","window_days":63,"op":"<","value":"0"}'::jsonb,
   0.3, '10y nominal trend 63d slope < 0');

-- supply_shock (regime_id=4)
INSERT INTO ontology.regime_triggers (regime_id, variable_id, condition, weight, description) VALUES
  (4, 5,
   '{"type":"compare","metric":"zscore","variable":"brent_oil","window_days":63,"op":">","value":"2.0"}'::jsonb,
   0.4, 'Brent z-score 3m > 2.0'),
  (4, 7,
   '{"type":"compare","metric":"yoy_change","variable":"headline_cpi_yoy","window_days":null,"op":">","value":"0.035"}'::jsonb,
   0.3, 'Headline CPI YoY > 3.5%'),
  (4, 7,
   '{"type":"compare_spread","left":{"metric":"zscore","variable":"headline_cpi_yoy","window_days":63},"right":{"metric":"zscore","variable":"core_cpi_yoy","window_days":63},"op":">","value":"0.5"}'::jsonb,
   0.3, 'Headline-core CPI z-score spread > 0.5');

COMMIT;
```

- [ ] **Step 2: Ensure `make seed-ontology` works**

Check if `Makefile` has a `seed-ontology` target. If it does, verify it runs `psql -f infrastructure/sql/seed/ontology.sql` (or equivalent). If it does not, add:

```makefile
seed-ontology:
	docker compose -f infrastructure/docker/docker-compose.yml exec -T timescaledb \
	  psql -U postgres -d markets -f /seed/ontology.sql || \
	  psql "$$DATABASE_URL" -f infrastructure/sql/seed/ontology.sql
```

If the file mount path differs, adjust. If unsure, use the second form directly.

- [ ] **Step 3: Apply the seed to a clean dev DB**

```bash
make down && make up && make migrate && make seed-ontology
```

Expected: seed applies with no errors.

- [ ] **Step 4: Verify the seed**

```bash
docker compose -f infrastructure/docker/docker-compose.yml exec -T timescaledb \
  psql -U postgres -d markets -c "SELECT COUNT(*) FROM ontology.variables;" \
                                 -c "SELECT COUNT(*) FROM ontology.regimes;" \
                                 -c "SELECT COUNT(*) FROM ontology.regime_triggers;"
```

Expected: 7, 4, 9.

```bash
docker compose -f infrastructure/docker/docker-compose.yml exec -T timescaledb \
  psql -U postgres -d markets -c "SELECT condition FROM ontology.regime_triggers LIMIT 1;"
```

Expected: prints JSON (not text).

- [ ] **Step 5: Commit**

```bash
git add infrastructure/sql/seed/ontology.sql Makefile
git commit -m "feat(ontology): seed 7 variables, 4 regimes, 9 JSONB triggers for Stage 4"
```

---

## Task 11: End-to-end integration test + contract fixture

**Files:**
- Create: `packages/markets-pipeline/tests/integration/test_stage4_end_to_end.py`
- Create: `packages/markets-pipeline/tests/integration/conftest.py` (or extend existing)

This test sets up a deterministic minimal dataset (no network), runs transforms, runs the classifier, asserts a `regime_states` row exists with the expected shape. It also defines the `stage4_seeded_fixtures` fixture used by Task 8's contract test.

- [ ] **Step 1: Write the integration test and fixture**

Create `packages/markets-pipeline/tests/integration/conftest.py` (if missing) or append to it. This defines `stage4_seeded_fixtures`, which is consumed by both this test and the classifier contract test.

```python
"""Shared fixtures for Stage 4 integration tests."""
from datetime import date, timedelta
from decimal import Decimal
import os
import pytest

from markets_pipeline.stores.timescale import TimescaleWarehouse
from markets_pipeline.transforms.zscore import ZScoreTransform
from markets_pipeline.transforms.yoy_change import YoYChangeTransform
from markets_pipeline.transforms.trend import TrendTransform


SERIES_BY_VAR = {
    "us_10y_treasury":  "fred:DGS10",
    "us_2y_treasury":   "fred:DGS2",
    "us_real_10y":      "fred:DFII10",
    "dxy":              "yfinance:DX-Y.NYB:close",
    "brent_oil":        "yfinance:BZ=F:close",
    "core_cpi_yoy":     "fred:CPILFESL",
    "headline_cpi_yoy": "fred:CPIAUCSL",
}


@pytest.fixture
def warehouse() -> TimescaleWarehouse:
    dsn = os.environ.get("DATABASE_URL") or "postgresql://postgres:your_password_here@localhost:5432/markets"
    return TimescaleWarehouse(dsn=dsn)


@pytest.fixture
def stage4_seeded_fixtures(warehouse):
    """Seed enough synthetic history for every variable so that the classifier
    can run at `good_date` and will raise InsufficientDataError at
    `insufficient_date` (before any data begins).
    """
    good_date = date(2026, 4, 15)
    insufficient_date = date(2026, 1, 1)  # before any data below
    start = date(2025, 3, 1)  # ~400 days before good_date

    # 1. Insert observations for all seven variables.
    rows: list[dict] = []
    days = (good_date - start).days + 1
    for var_name, sid in SERIES_BY_VAR.items():
        for i in range(days):
            d = start + timedelta(days=i)
            # Use distinct synthetic patterns so computed metrics are well-defined.
            if var_name in ("us_real_10y",):
                value = Decimal(str(0.5 + (i - days // 2) * 0.005))  # rising through zero
            elif var_name == "dxy":
                value = Decimal(str(100 + i * 0.05))  # trending up
            elif var_name == "us_10y_treasury":
                value = Decimal(str(4.0 + i * 0.002))
            elif var_name == "us_2y_treasury":
                value = Decimal(str(4.2))
            elif var_name == "brent_oil":
                value = Decimal(str(80 + (i % 30) * 0.2))  # oscillates, no big z-score
            elif var_name == "core_cpi_yoy":
                # monthly series: only insert on the 1st of each month
                if d.day != 1:
                    continue
                value = Decimal(str(0.03 + i * 0.00001))
            elif var_name == "headline_cpi_yoy":
                if d.day != 1:
                    continue
                value = Decimal(str(0.03 + i * 0.00001))
            rows.append({
                "series_id": sid, "observation_date": d, "as_of_date": d,
                "value": value, "source_revision": None,
            })
    warehouse.upsert_observations(rows)

    # 2. Run transforms for good_date so the classifier has features.
    ZScoreTransform(warehouse).compute(good_date)
    YoYChangeTransform(warehouse).compute(good_date)
    TrendTransform(warehouse).compute(good_date)

    yield {"good_date": good_date, "insufficient_date": insufficient_date,
           "series_ids": list(SERIES_BY_VAR.values())}

    # Cleanup
    for sid in SERIES_BY_VAR.values():
        warehouse.execute_sql("DELETE FROM observations.observations WHERE series_id = %s", (sid,))
        warehouse.execute_sql("DELETE FROM derived.zscores WHERE series_id = %s", (sid,))
        warehouse.execute_sql("DELETE FROM derived.yoy_changes WHERE series_id = %s", (sid,))
        warehouse.execute_sql("DELETE FROM derived.trends WHERE series_id = %s", (sid,))
    warehouse.execute_sql(
        "DELETE FROM regimes.regime_states WHERE as_of_date = %s", (good_date,)
    )
```

Create `packages/markets-pipeline/tests/integration/test_stage4_end_to_end.py`:

```python
"""End-to-end smoke test for Stage 4.

Assumes `make seed-ontology` has been run against the test DB so that the
7 variables + 4 regimes + 9 triggers are present.
"""
from decimal import Decimal
import pytest

from markets_pipeline.classifiers.rule_based import RuleBasedClassifier


pytestmark = pytest.mark.integration


def test_classifier_produces_valid_regime_state(warehouse, stage4_seeded_fixtures):
    good = stage4_seeded_fixtures["good_date"]
    clf = RuleBasedClassifier(warehouse=warehouse)
    result = clf.classify(good)
    assert result.as_of_date == good
    assert result.regime_name in {
        "benign_expansion", "monetary_tightening", "monetary_easing", "supply_shock"
    }
    assert Decimal("0") <= result.confidence <= Decimal("1")
    assert result.classifier_name == "rule_based"
    assert result.classifier_version == "1.0.0"
    assert result.ontology_version == "002"
    assert "triggers" in result.rationale_detail


def test_classifier_writes_to_regime_states(warehouse, stage4_seeded_fixtures):
    good = stage4_seeded_fixtures["good_date"]
    clf = RuleBasedClassifier(warehouse=warehouse)
    result = clf.classify(good)
    warehouse.upsert_regime_state({
        "as_of_date": result.as_of_date,
        "classifier_name": result.classifier_name,
        "classifier_version": result.classifier_version,
        "regime_name": result.regime_name,
        "confidence": result.confidence,
        "trigger_variables": result.trigger_variables,
        "rationale": result.rationale,
        "rationale_detail": result.rationale_detail,
        "ontology_version": result.ontology_version,
    })

    with warehouse._conn() as conn, conn.cursor() as cur:
        cur.execute(
            "SELECT regime_name, ontology_version, rationale_detail "
            "FROM regimes.regime_states WHERE as_of_date = %s "
            "  AND classifier_name = %s AND classifier_version = %s",
            (good, "rule_based", "1.0.0"),
        )
        row = cur.fetchone()
        assert row is not None
        assert row[1] == "002"
        assert "winner" in row[2]


def test_classifier_idempotent_on_same_as_of(warehouse, stage4_seeded_fixtures):
    good = stage4_seeded_fixtures["good_date"]
    clf = RuleBasedClassifier(warehouse=warehouse)
    a = clf.classify(good)
    b = clf.classify(good)
    assert a == b
```

- [ ] **Step 2: Run the test**

```bash
make down && make up && make migrate && make seed-ontology
pytest packages/markets-pipeline/tests/integration/test_stage4_end_to_end.py \
       packages/markets-pipeline/tests/contract/test_rule_based_contract.py \
       -v -m integration
```

Expected: all PASS.

- [ ] **Step 3: Commit**

```bash
git add packages/markets-pipeline/tests/integration/conftest.py \
        packages/markets-pipeline/tests/integration/test_stage4_end_to_end.py
git commit -m "test(stage4): end-to-end smoke + classifier contract fixture"
```

---

## Task 12: Airflow DAGs + ingest yfinance DAG

**Files:**
- Modify: `packages/markets-pipeline/src/markets_pipeline/dags/derive.py`
- Modify: `packages/markets-pipeline/src/markets_pipeline/dags/classify.py`
- Create: `packages/markets-pipeline/src/markets_pipeline/dags/ingest_yfinance.py`

Existing `derive.py` and `classify.py` are stubs referencing classes we've now built. Rewrite them to:
- `derive.py` runs all three transforms sequentially for `logical_date`.
- `classify.py` instantiates `RuleBasedClassifier` and writes to `regime_states`.
- `ingest_yfinance.py` pulls DXY + Brent close-channel observations via the minimal yfinance source and writes to Parquet (via the existing `ParquetRawStore`) and Timescale.

- [ ] **Step 1: Rewrite `derive.py`**

Overwrite `packages/markets-pipeline/src/markets_pipeline/dags/derive.py`:

```python
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.datasets import Dataset
import os

from markets_pipeline.stores.timescale import TimescaleWarehouse
from markets_pipeline.transforms.zscore import ZScoreTransform
from markets_pipeline.transforms.yoy_change import YoYChangeTransform
from markets_pipeline.transforms.trend import TrendTransform


default_args = {
    "owner": "markets",
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
}

observations_dataset = Dataset("s3://markets-observations")
derived_dataset = Dataset("s3://markets-derived")


def _warehouse() -> TimescaleWarehouse:
    dsn = os.environ.get("DATABASE_URL") or "postgresql://postgres:your_password_here@localhost:5432/markets"
    return TimescaleWarehouse(dsn=dsn)


def derive_all_task(logical_date: str) -> None:
    d = datetime.fromisoformat(logical_date).date()
    wh = _warehouse()
    ZScoreTransform(wh).compute(d)
    YoYChangeTransform(wh).compute(d)
    TrendTransform(wh).compute(d)


dag = DAG(
    "derive",
    default_args=default_args,
    description="Derive analytical views (z-score, YoY, trend)",
    schedule=[observations_dataset],
    start_date=datetime(2026, 1, 1),
    catchup=False,
    outlets=[derived_dataset],
)

PythonOperator(
    task_id="derive_all",
    python_callable=derive_all_task,
    op_kwargs={"logical_date": "{{ logical_date.isoformat() }}"},
    outlets=[derived_dataset],
    dag=dag,
)
```

- [ ] **Step 2: Rewrite `classify.py`**

Overwrite `packages/markets-pipeline/src/markets_pipeline/dags/classify.py`:

```python
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.datasets import Dataset
import os

from markets_pipeline.stores.timescale import TimescaleWarehouse
from markets_pipeline.classifiers.rule_based import RuleBasedClassifier


default_args = {
    "owner": "markets",
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
}

derived_dataset = Dataset("s3://markets-derived")


def _warehouse() -> TimescaleWarehouse:
    dsn = os.environ.get("DATABASE_URL") or "postgresql://postgres:your_password_here@localhost:5432/markets"
    return TimescaleWarehouse(dsn=dsn)


def classify_task(logical_date: str) -> None:
    d = datetime.fromisoformat(logical_date).date()
    wh = _warehouse()
    clf = RuleBasedClassifier(warehouse=wh)
    result = clf.classify(d)
    wh.upsert_regime_state({
        "as_of_date": result.as_of_date,
        "classifier_name": result.classifier_name,
        "classifier_version": result.classifier_version,
        "regime_name": result.regime_name,
        "confidence": result.confidence,
        "trigger_variables": result.trigger_variables,
        "rationale": result.rationale,
        "rationale_detail": result.rationale_detail,
        "ontology_version": result.ontology_version,
    })


dag = DAG(
    "classify",
    default_args=default_args,
    description="Classify market regimes",
    schedule=[derived_dataset],
    start_date=datetime(2026, 1, 1),
    catchup=False,
)

PythonOperator(
    task_id="classify_regimes",
    python_callable=classify_task,
    op_kwargs={"logical_date": "{{ logical_date.isoformat() }}"},
    dag=dag,
)
```

- [ ] **Step 3: Create `ingest_yfinance.py`**

Create `packages/markets-pipeline/src/markets_pipeline/dags/ingest_yfinance.py`:

```python
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.datasets import Dataset
import os

from markets_core.domain.series import SeriesId

from markets_pipeline.sources.yfinance import YfinanceSource
from markets_pipeline.stores.timescale import TimescaleWarehouse


default_args = {
    "owner": "markets",
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
}

observations_dataset = Dataset("s3://markets-observations")

SERIES = [
    "yfinance:DX-Y.NYB:close",
    "yfinance:BZ=F:close",
]


def _warehouse() -> TimescaleWarehouse:
    dsn = os.environ.get("DATABASE_URL") or "postgresql://postgres:your_password_here@localhost:5432/markets"
    return TimescaleWarehouse(dsn=dsn)


def ingest_yfinance_task(logical_date: str) -> None:
    d = datetime.fromisoformat(logical_date).date()
    src = YfinanceSource()
    wh = _warehouse()
    rows = []
    for canonical in SERIES:
        _, rest = canonical.split(":", 1)  # "DX-Y.NYB:close"
        sid = SeriesId(source="yfinance", native_id=rest)
        for obs in src.fetch(series_id=sid, start=d, end=d + timedelta(days=1), as_of=d):
            rows.append({
                "series_id": canonical,
                "observation_date": obs.observation_date,
                "as_of_date": obs.as_of_date,
                "value": obs.value,
                "source_revision": obs.source_revision,
            })
    wh.upsert_observations(rows)


dag = DAG(
    "ingest_yfinance",
    default_args=default_args,
    description="Ingest daily OHLC from yfinance (DXY, Brent)",
    schedule="@daily",
    start_date=datetime(2026, 1, 1),
    catchup=False,
)

PythonOperator(
    task_id="ingest_yfinance",
    python_callable=ingest_yfinance_task,
    op_kwargs={"logical_date": "{{ logical_date.isoformat() }}"},
    outlets=[observations_dataset],
    dag=dag,
)
```

- [ ] **Step 4: Syntax-check DAG files**

```bash
python -c "import ast, pathlib; [ast.parse(p.read_text()) for p in pathlib.Path('packages/markets-pipeline/src/markets_pipeline/dags').glob('*.py')]"
```

Expected: no output (all parse cleanly).

- [ ] **Step 5: Commit**

```bash
git add packages/markets-pipeline/src/markets_pipeline/dags/
git commit -m "feat(dags): rewrite derive and classify DAGs; add ingest_yfinance DAG"
```

---

## Task 13: Backfill command + Makefile target

**Files:**
- Create: `scripts/backfill_classify.py`
- Modify: `Makefile`

- [ ] **Step 1: Write the backfill script**

Create `scripts/backfill_classify.py`:

```python
"""Replay classifier over a historical date range.

Usage:
    python scripts/backfill_classify.py --start 2020-01-01 --end 2026-04-22

Reads DATABASE_URL from env. Idempotent: upserts on
(as_of_date, classifier_name, classifier_version).
"""
from __future__ import annotations
import argparse
import os
import sys
from datetime import date, timedelta

from markets_pipeline.classifiers.rule_based import RuleBasedClassifier
from markets_pipeline.stores.timescale import TimescaleWarehouse
from markets_core.errors import InsufficientDataError


def _parse_date(s: str) -> date:
    return date.fromisoformat(s)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--start", type=_parse_date, required=True)
    parser.add_argument("--end", type=_parse_date, required=True)
    parser.add_argument(
        "--skip-insufficient",
        action="store_true",
        help="Continue past InsufficientDataError instead of aborting.",
    )
    args = parser.parse_args(argv)

    dsn = os.environ["DATABASE_URL"]
    wh = TimescaleWarehouse(dsn=dsn)
    clf = RuleBasedClassifier(warehouse=wh)

    d = args.start
    attempted = 0
    written = 0
    skipped = 0
    while d <= args.end:
        # Business-day filter: Monday=0 ... Friday=4
        if d.weekday() < 5:
            attempted += 1
            try:
                result = clf.classify(d)
                wh.upsert_regime_state({
                    "as_of_date": result.as_of_date,
                    "classifier_name": result.classifier_name,
                    "classifier_version": result.classifier_version,
                    "regime_name": result.regime_name,
                    "confidence": result.confidence,
                    "trigger_variables": result.trigger_variables,
                    "rationale": result.rationale,
                    "rationale_detail": result.rationale_detail,
                    "ontology_version": result.ontology_version,
                })
                written += 1
            except InsufficientDataError as e:
                if not args.skip_insufficient:
                    print(f"ERROR: insufficient data at {d}: {e}", file=sys.stderr)
                    return 2
                skipped += 1
        d = d + timedelta(days=1)
    print(f"backfill_classify: attempted={attempted} written={written} skipped={skipped}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
```

- [ ] **Step 2: Add Makefile target**

Append to `Makefile`:

```makefile
backfill-classify:
	@test -n "$(START)" || (echo "Usage: make backfill-classify START=YYYY-MM-DD END=YYYY-MM-DD [EXTRA='--skip-insufficient']" && exit 1)
	@test -n "$(END)"   || (echo "Usage: make backfill-classify START=YYYY-MM-DD END=YYYY-MM-DD [EXTRA='--skip-insufficient']" && exit 1)
	python scripts/backfill_classify.py --start $(START) --end $(END) $(EXTRA)

test-stage4:
	pytest packages/markets-core/tests/unit/ \
	       packages/markets-pipeline/tests/unit/test_conditions.py \
	       packages/markets-pipeline/tests/unit/test_transforms_math.py \
	       packages/markets-pipeline/tests/unit/test_rule_based_scoring.py \
	       -v
	pytest packages/markets-pipeline/tests/integration/test_stage4_end_to_end.py \
	       packages/markets-pipeline/tests/contract/test_rule_based_contract.py \
	       packages/markets-pipeline/tests/contract/test_transforms_contract.py \
	       -v -m integration
```

- [ ] **Step 3: Smoke-test the backfill script**

```bash
make up && make migrate && make seed-ontology
# Seed enough fake history (reuse the end-to-end fixture by running its test once):
pytest packages/markets-pipeline/tests/integration/test_stage4_end_to_end.py::test_classifier_produces_valid_regime_state -m integration

# Now the DB has fake history; run backfill over a 3-day window
DATABASE_URL="postgresql://postgres:your_password_here@localhost:5432/markets" \
  python scripts/backfill_classify.py --start 2026-04-13 --end 2026-04-15 --skip-insufficient
```

Expected: exits 0 and prints `backfill_classify: attempted=3 written=<0..3> skipped=<...>`. The exact counts depend on what history the synthetic fixture left behind — what matters is that the command runs cleanly without crashing.

- [ ] **Step 4: Commit**

```bash
git add scripts/backfill_classify.py Makefile
git commit -m "feat(ops): add backfill-classify command and test-stage4 Makefile target"
```

---

## Task 14: Historical regression smoke tests

**Files:**
- Create: `packages/markets-pipeline/tests/integration/test_historical_regression.py`

Per spec §7.4: three hand-picked episodes. These are smoke tests, not a calibration suite. They require real FRED + yfinance data, so they're marked `@pytest.mark.slow` in addition to `@pytest.mark.integration`, and they should only run when `FRED_API_KEY` is set.

Because running these requires the full ingest chain (FRED + yfinance + transforms + classifier) to have populated enough history up to each episode's date, the test uses a "self-contained run" pattern: fetch data for each episode's required window, ingest, transform, classify, then assert.

- [ ] **Step 1: Write the test**

Create `packages/markets-pipeline/tests/integration/test_historical_regression.py`:

```python
"""Historical regression smoke tests (§7.4 of spec).

These are smoke tests for design correctness, NOT a calibration suite.
If an episode fails, either tune thresholds in the ontology seed OR
relax the assertion with a `# TODO: calibrate` note and open a follow-up issue.

Requires FRED_API_KEY and internet. Marked @pytest.mark.slow.
"""
from datetime import date, timedelta
from decimal import Decimal
import os
import pytest

from markets_core.domain.series import SeriesId

from markets_pipeline.classifiers.rule_based import RuleBasedClassifier
from markets_pipeline.sources.fred import FredSource
from markets_pipeline.sources.yfinance import YfinanceSource
from markets_pipeline.stores.timescale import TimescaleWarehouse
from markets_pipeline.transforms.zscore import ZScoreTransform
from markets_pipeline.transforms.yoy_change import YoYChangeTransform
from markets_pipeline.transforms.trend import TrendTransform


pytestmark = [pytest.mark.integration, pytest.mark.slow]


SERIES_TO_FETCH: list[tuple[str, str]] = [
    ("fred", "DGS10"),
    ("fred", "DGS2"),
    ("fred", "DFII10"),
    ("fred", "CPILFESL"),
    ("fred", "CPIAUCSL"),
    ("yfinance", "DX-Y.NYB:close"),
    ("yfinance", "BZ=F:close"),
]


EPISODES = [
    (date(2020, 4, 15), "monetary_easing"),
    (date(2022, 10, 15), "monetary_tightening"),
    (date(2022, 3, 15), "supply_shock"),
]


@pytest.fixture(scope="module")
def warehouse() -> TimescaleWarehouse:
    dsn = os.environ.get("DATABASE_URL") or "postgresql://postgres:your_password_here@localhost:5432/markets"
    return TimescaleWarehouse(dsn=dsn)


def _ingest_window(warehouse, start: date, end: date) -> None:
    fred_key = os.environ["FRED_API_KEY"]
    fred = FredSource(api_key=fred_key)
    yf = YfinanceSource()
    rows: list[dict] = []
    for source, native in SERIES_TO_FETCH:
        sid = SeriesId(source=source, native_id=native)
        canonical = f"{source}:{native}"
        src = fred if source == "fred" else yf
        for obs in src.fetch(sid, start=start, end=end):
            rows.append({
                "series_id": canonical,
                "observation_date": obs.observation_date,
                "as_of_date": obs.as_of_date,
                "value": obs.value,
                "source_revision": obs.source_revision,
            })
    warehouse.upsert_observations(rows)


@pytest.mark.skipif(not os.environ.get("FRED_API_KEY"),
                    reason="FRED_API_KEY not set")
@pytest.mark.parametrize("as_of,expected_regime", EPISODES)
def test_historical_episode(warehouse, as_of: date, expected_regime: str):
    start = as_of - timedelta(days=400)
    end = as_of + timedelta(days=1)
    _ingest_window(warehouse, start, end)
    ZScoreTransform(warehouse).compute(as_of)
    YoYChangeTransform(warehouse).compute(as_of)
    TrendTransform(warehouse).compute(as_of)
    clf = RuleBasedClassifier(warehouse=warehouse)
    result = clf.classify(as_of)
    # Diagnostic output if it fails
    assert result.regime_name == expected_regime, (
        f"Expected {expected_regime} on {as_of}, got {result.regime_name}. "
        f"Scores: {result.rationale_detail.get('regime_scores')}"
    )
    assert result.confidence > Decimal("0.5")
```

- [ ] **Step 2: Register the `slow` marker**

If `pytest.ini` or `pyproject.toml` doesn't already register the `slow` marker, add it under `[tool.pytest.ini_options]` (or equivalent):

```toml
markers = [
    "integration: requires external services",
    "slow: requires real API calls and lengthy data fetch",
]
```

- [ ] **Step 3: Run (optional, requires FRED_API_KEY + network)**

```bash
FRED_API_KEY=<key> pytest packages/markets-pipeline/tests/integration/test_historical_regression.py -v -m "slow and integration"
```

Any failures here are informational: document in the PR description which episodes pass and which need calibration. Do NOT gate the merge on all three passing — the spec explicitly allows relaxation.

If an episode fails and you can improve it quickly by adjusting a threshold in the seed, do so. If not, relax the assertion (`pytest.xfail` with a reason) and file a follow-up.

- [ ] **Step 4: Commit**

```bash
git add packages/markets-pipeline/tests/integration/test_historical_regression.py \
        pyproject.toml  # if marker added there
git commit -m "test(stage4): historical regression smoke tests (2020 easing, 2022 tightening, 2022 supply shock)"
```

---

## Task 15: Documentation updates

**Files:**
- Modify: `CLAUDE.md`
- Modify: `docs/DATA-MODEL.md`
- Modify: `docs/ONTOLOGY.md`

- [ ] **Step 1: Add "No sub-daily data" to CLAUDE.md**

In `CLAUDE.md`, under the `## Non-negotiable rules` section, add this bullet after the "Never commit `.env` files" bullet:

```markdown
- **No sub-daily data.** All series are stored at daily granularity or coarser. Market variables that natively trade intraday are stored as daily OHLC (open/high/low/close). Lower-frequency series (e.g., monthly CPI) remain at native frequency in storage and are forward-filled to daily only at the classifier/transform read boundary. Intraday data is never ingested.
```

- [ ] **Step 2: Update DATA-MODEL.md**

In `docs/DATA-MODEL.md`, in the section describing `ontology.regime_triggers`, change the condition column description from `text` (DSL string) to `jsonb` (validated AST). Add a note under the table describing the AST shape (or link to the spec):

```markdown
Note: as of migration 002, `condition` is `jsonb`, not `text`. It holds a
validated AST with node types `compare`, `compare_spread`, `all_of`, `any_of`.
See `docs/superpowers/specs/2026-04-22-stage-4-classifier-design.md` §4.
```

Similarly in the `regimes.regime_states` section, document the new columns:

```markdown
As of migration 002, `regime_states` has two additional columns:
- `rationale_detail jsonb NOT NULL` — structured per-trigger breakdown produced by
  the classifier. Schema documented in spec §5.2.
- `ontology_version text NOT NULL` — the Alembic revision of the ontology used
  to produce this row. Two rows with the same `(classifier_version, ontology_version)`
  are guaranteed reproducible.
```

- [ ] **Step 3: Update ONTOLOGY.md**

In `docs/ONTOLOGY.md`, in the "Named regimes in the initial definition set" section, prepend a note:

```markdown
**Initial seeded set (as of migration 002):** `benign_expansion`,
`monetary_tightening`, `monetary_easing`, `supply_shock`. The five remaining
regimes below (`fiscal_dominance`, `funding_stress`, `emerging_market_crisis`,
`institutional_shock`, `phase_transition`) are documented but not yet seeded;
adding them is an additive ontology migration per the evolution policy.
```

- [ ] **Step 4: Commit**

```bash
git add CLAUDE.md docs/DATA-MODEL.md docs/ONTOLOGY.md
git commit -m "docs: add no-sub-daily-data rule; document jsonb condition and regime_states columns"
```

---

## Task 16: Final verification

- [ ] **Step 1: Full unit + contract test run**

```bash
make up && make migrate && make seed-ontology
pytest packages/markets-core/tests/unit/ packages/markets-pipeline/tests/unit/ -v
```

Expected: all PASS.

- [ ] **Step 2: Full integration test run**

```bash
pytest -v -m integration \
  packages/markets-pipeline/tests/integration/ \
  packages/markets-pipeline/tests/contract/
```

Expected: all PASS.

- [ ] **Step 3: Lint + type check**

```bash
make lint
```

Expected: clean (or pre-existing warnings only, nothing new introduced by this work).

- [ ] **Step 4: Verify dev DAGs import cleanly in Airflow**

```bash
docker compose -f infrastructure/docker/docker-compose.yml exec airflow-scheduler \
  airflow dags list-import-errors
```

Expected: no errors for `derive`, `classify`, `ingest_yfinance`.

- [ ] **Step 5: Tag and summarize**

Write a short summary to the PR description listing:
- Which historical regression episodes passed (from Task 14).
- Any calibration TODOs deferred.
- Any pre-existing test failures explicitly not touched.

---

## Self-Review Checklist (performed by plan author)

**Spec coverage** — every section of `2026-04-22-stage-4-classifier-design.md` is covered:

- §1 Goals → all tasks collectively; §2 — Task 15 updates CLAUDE.md.
- §3.1 core abstractions → Tasks 1 (`InsufficientDataError`), 2 (domain models), 3 (Classifier protocol + contract).
- §3.2 pipeline implementations → Tasks 6 (conditions), 7 (transforms), 8 (rule_based), 9 (yfinance), 12 (DAGs), 13 (backfill).
- §3.3 SQL → Task 4 (migration), Task 10 (seed).
- §3.4 data flow — Task 11 exercises it end to end.
- §4 AST → Task 2 defines models; Task 6 evaluates.
- §5 schemas → Task 4 migration + Task 5 warehouse methods.
- §6 seed → Task 10.
- §7 testing → Tasks 2, 6-9, 11, 14.
- §8 errors → Task 1 + enforced in Tasks 6, 8.
- §9 operations → Tasks 12, 13, 15.
- §10 component inventory → tasks are organized along these lines.
- §11 out-of-scope — noted, no tasks.

**Placeholder scan:** no "TBD" / "TODO later" / "similar to above" without content. One placeholder in Task 9 (migration revision format in filename `002_*`) is legitimate. Task 14 allows for `pytest.xfail` with reason if an episode needs tuning — this is explicit and documented, not a hidden TODO.

**Type consistency checked:**
- `RegimeClassification` fields consistent between Task 2 (definition) and Task 8 (construction).
- `TriggerEvaluation` fields consistent: Task 2 defines them, Task 8 populates them.
- Warehouse method names consistent between Task 5 (definition) and callers in Tasks 7, 8, 11, 12, 13.
- `feature_key` signature consistent: Task 6 defines `(metric, variable, window_days)` → str; Task 8 uses the same.
- `pick_winner` signature consistent: Task 8 tests assert `(winner, confidence, tiebreak)` triple; implementation returns same.

One issue fixed inline: in the spec §6.1 primary_series was listed without `:close` suffix for yfinance variables; I've adjusted the seed (Task 10) and the yfinance source (Task 9) so the explicit OHLC-channel naming is consistent everywhere — this is the only deviation from spec §6.1 and it's justified in Task 9's preamble.
