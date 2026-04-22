import pytest
from datetime import date
from decimal import Decimal

from markets_pipeline.classifiers.rule_based import RuleBasedClassifier


class MockWarehouse:
    """Mock warehouse for testing."""
    
    def query_regime_triggers(self):
        """Return mock regime triggers."""
        return [
            {
                "regime_id": 1,
                "variable_id": 1,
                "regime_name": "benign_expansion",
                "variable_name": "us_10y_treasury",
                "condition": "zscore_3m <= 1.0",
                "weight": Decimal("1.0"),
            },
            {
                "regime_id": 2,
                "variable_id": 1,
                "regime_name": "monetary_tightening",
                "variable_name": "us_10y_treasury",
                "condition": "zscore_3m > 2.0",
                "weight": Decimal("1.0"),
            },
        ]
    
    def query_zscores(self, series_id, as_of, window_days):
        """Return mock z-scores."""
        return {
            date(2026, 3, 15): Decimal("0.5"),  # Low z-score
        }


def test_classify_benign():
    """Test classification as benign expansion."""
    warehouse = MockWarehouse()
    classifier = RuleBasedClassifier(warehouse=warehouse)
    
    states = classifier.classify(as_of=date(2026, 3, 15))
    
    # Should have one regime state
    assert len(states) > 0
    assert states[0]["regime_name"] == "benign_expansion"
    assert states[0]["confidence"] == Decimal("1.0")


def test_evaluate_condition():
    """Test condition evaluation."""
    warehouse = MockWarehouse()
    classifier = RuleBasedClassifier(warehouse=warehouse)
    
    assert classifier._evaluate_condition("zscore_3m > 2.0", 3.0) == True
    assert classifier._evaluate_condition("zscore_3m > 2.0", 1.0) == False
    assert classifier._evaluate_condition("zscore_3m <= 1.0", 0.5) == True