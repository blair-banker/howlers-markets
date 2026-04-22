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
