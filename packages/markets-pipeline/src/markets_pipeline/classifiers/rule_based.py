"""Rule-based regime classifier."""
from __future__ import annotations
from datetime import date
from decimal import Decimal
from typing import Any

from markets_core.domain.regime import (
    CompareNode,
    CompareSpreadNode,
    MetricRef,
    RegimeClassification,
    RegimeTrigger,
    TriggerEvaluation,
    parse_condition,
)
from markets_core.errors import ConfigurationError

from markets_pipeline.classifiers.conditions import (
    evaluate,
    feature_key,
    required_features,
)


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
        top.sort(key=lambda n: (-total_weights.get(n, Decimal("0")), n))
    return top[0], max_score, tiebreak


class RuleBasedClassifier:
    """Classifier implementation. Name 'rule_based', version '1.0.0'."""

    name: str = "rule_based"
    version: str = "1.0.0"

    def __init__(self, warehouse: Any) -> None:
        self.warehouse = warehouse
        self._load_ontology()

    def _load_ontology(self) -> None:
        self._ontology_version = self.warehouse.read_ontology_version()
        variables = self.warehouse.read_ontology_variables()
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
        evaluations: list[TriggerEvaluation] = []
        per_regime: dict[str, list[dict[str, Any]]] = {
            n: [] for n in self._regime_id_to_name.values()
        }
        for trig in self._triggers:
            regime_name = self._regime_id_to_name[trig.regime_id]
            satisfied = evaluate(trig.condition, features)
            evaluations.append(
                self._build_evaluation(trig, regime_name, satisfied, features)
            )
            per_regime[regime_name].append(
                {"weight": trig.weight, "satisfied": satisfied}
            )

        scores: dict[str, Decimal] = {}
        total_weights: dict[str, Decimal] = {}
        for name in self._stress_regime_names:
            triggers_for_regime = per_regime.get(name, [])
            scores[name] = weighted_score(triggers_for_regime)
            total_weights[name] = sum(
                (t["weight"] for t in triggers_for_regime), start=Decimal("0")
            )

        winner, confidence, tiebreak = pick_winner(scores, total_weights, BENIGN_THRESHOLD)

        if winner == BENIGN_REGIME_NAME:
            trigger_variables: list[str] = []
        else:
            trigger_variables = sorted({
                self._var_id_to_name[t.variable_id]
                for t in self._triggers
                if self._regime_id_to_name[t.regime_id] == winner
                and any(e.trigger_id == t.id and e.satisfied for e in evaluations)
            })

        rationale = self._build_rationale(winner, confidence, per_regime, tiebreak)
        rationale_detail: dict[str, Any] = {
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
