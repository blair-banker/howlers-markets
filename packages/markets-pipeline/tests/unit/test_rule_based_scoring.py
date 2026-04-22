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
    assert confidence == Decimal("0.70")
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
    assert winner == "monetary_tightening"
    assert tiebreak is True
