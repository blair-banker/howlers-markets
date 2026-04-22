"""End-to-end smoke test for Stage 4.

Assumes ``make seed-ontology`` has been run against the test DB so that the
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

    import psycopg2
    conn = psycopg2.connect(warehouse.dsn)
    try:
        cur = conn.cursor()
        cur.execute(
            "SELECT regime_name, ontology_version, rationale_detail "
            "FROM regimes.regime_states WHERE as_of_date = %s "
            "  AND classifier_name = %s AND classifier_version = %s",
            (good, "rule_based", "1.0.0"),
        )
        row = cur.fetchone()
        cur.close()
    finally:
        conn.close()
    assert row is not None
    assert row[1] == "002"
    assert "winner" in row[2]


def test_classifier_idempotent_on_same_as_of(warehouse, stage4_seeded_fixtures):
    good = stage4_seeded_fixtures["good_date"]
    clf = RuleBasedClassifier(warehouse=warehouse)
    a = clf.classify(good)
    b = clf.classify(good)
    assert a == b
