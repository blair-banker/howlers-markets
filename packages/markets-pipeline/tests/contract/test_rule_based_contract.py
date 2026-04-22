"""Integration contract test for RuleBasedClassifier.

The `stage4_seeded_fixtures` fixture is defined in Task 11's integration
conftest (packages/markets-pipeline/tests/integration/conftest.py). Until
that fixture is in place, this file's tests will be deselected or errored --
that's expected. Task 11 makes it work.
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
        return stage4_seeded_fixtures["good_date"]

    @pytest.fixture
    def known_insufficient_as_of_date(self, stage4_seeded_fixtures) -> date:
        return stage4_seeded_fixtures["insufficient_date"]
