"""Historical regression smoke tests (§7.4 of spec).

These are smoke tests for design correctness, NOT a calibration suite.
If an episode fails, either tune thresholds in the ontology seed OR
mark the assertion with ``pytest.xfail(reason=...)`` and open a follow-up.

Requires FRED_API_KEY and internet access. Marked @pytest.mark.slow.
"""
from datetime import date, timedelta
from decimal import Decimal
import os
import pytest

from markets_core.domain.observation import Observation
from markets_core.domain.series import SeriesId

from markets_pipeline.classifiers.rule_based import RuleBasedClassifier
from markets_pipeline.sources.fred import FredSource
from markets_pipeline.sources.yfinance import YfinanceSource
from markets_pipeline.stores.timescale import TimescaleWarehouse
from markets_pipeline.transforms.zscore import ZScoreTransform
from markets_pipeline.transforms.yoy_change import YoYChangeTransform
from markets_pipeline.transforms.trend import TrendTransform


pytestmark = [pytest.mark.integration, pytest.mark.slow]


# (source_name, native_id, canonical_series_id)
SERIES_TO_FETCH: list[tuple[str, str, str]] = [
    ("fred", "DGS10", "fred:DGS10"),
    ("fred", "DGS2", "fred:DGS2"),
    ("fred", "DFII10", "fred:DFII10"),
    ("fred", "CPILFESL", "fred:CPILFESL"),
    ("fred", "CPIAUCSL", "fred:CPIAUCSL"),
    ("yfinance", "DX-Y.NYB:close", "yfinance:DX-Y.NYB:close"),
    ("yfinance", "BZ=F:close", "yfinance:BZ=F:close"),
]

# Unique namespace prefix to avoid collisions with synthetic test data.
# All series use their real canonical IDs; cleanup targets only rows
# written during the module's run (by observation_date range + series_id).
_INGEST_TAG = "hist_regression"

EPISODES = [
    (date(2020, 4, 15), "monetary_easing"),
    (date(2022, 10, 15), "monetary_tightening"),
    # TODO: calibrate — 2022-03-15 is only 3 weeks after Russia's Ukraine invasion.
    # The 63-day Brent z-score window still includes pre-invasion prices, so
    # brent_oil zscore = 0.96 (threshold is > 2.0).  Additionally the
    # headline-core CPI z-score spread trigger returns None (insufficient
    # within-window CPI monthly data to compute both zscores independently).
    # Actual regime_scores: monetary_tightening=0.3, supply_shock=0.3 → benign wins.
    # Fix options: (a) lower brent zscore threshold to 1.0, (b) use 2022-04-15
    # as the episode date (4 more weeks for the oil spike to register), or
    # (c) fix the spread trigger to handle sparse monthly series.
    pytest.param(
        date(2022, 3, 15),
        "supply_shock",
        marks=pytest.mark.xfail(
            reason=(
                "TODO: calibrate — brent_oil zscore only 0.96 (< 2.0 threshold) "
                "3 weeks after Ukraine invasion; headline-core CPI spread trigger "
                "returns None for sparse monthly data. "
                "Got benign_expansion; regime_scores: "
                "{'monetary_tightening': '0.3', 'supply_shock': '0.3'}."
            ),
            strict=False,
        ),
    ),
]


def _dsn() -> str:
    return (
        os.environ.get("DATABASE_URL")
        or "postgresql://postgres:your_password_here@localhost:5432/markets"
    )


@pytest.fixture(scope="module")
def warehouse() -> TimescaleWarehouse:
    return TimescaleWarehouse(dsn=_dsn())


def _ingest_window(
    warehouse: TimescaleWarehouse,
    start: date,
    end: date,
    as_of: date,
) -> None:
    """Fetch all required series over [start, end] and upsert to warehouse.

    ``as_of`` is passed to each source so that observations are stored with
    a historical vintage date (as_of <= target classification date) rather
    than today's wall-clock date.  This is required for the point-in-time
    filter ``as_of_date <= :as_of_date`` to return any rows when classifying
    at a historical date.
    """
    fred_key = os.environ["FRED_API_KEY"]
    fred = FredSource(api_key=fred_key)
    yf = YfinanceSource()
    observations: list[Observation] = []

    for source_name, native_id, canonical in SERIES_TO_FETCH:
        sid = SeriesId(source=source_name, native_id=native_id)
        src = fred if source_name == "fred" else yf
        try:
            for obs in src.fetch(sid, start=start, end=end, as_of=as_of):
                # Ensure the series_id is exactly the canonical form expected
                # by the transforms and classifier.
                if obs.series_id != canonical:
                    obs = obs.model_copy(update={"series_id": canonical})
                observations.append(obs)
        except Exception as e:
            pytest.fail(
                f"ingest failed for {source_name}:{native_id} "
                f"[{start} – {end}]: {e}"
            )

    warehouse.upsert_observations(observations)


def _cleanup_window(
    warehouse: TimescaleWarehouse,
    start: date,
    end: date,
    as_of: date,
) -> None:
    """Remove observations and derived rows written for this episode."""
    ext_end = end + timedelta(days=1)
    for _, _, canonical in SERIES_TO_FETCH:
        warehouse.execute_sql(
            "DELETE FROM observations.observations "
            "WHERE series_id = %s "
            "  AND observation_date BETWEEN %s AND %s",
            (canonical, start, ext_end),
        )
        warehouse.execute_sql(
            "DELETE FROM derived.zscores "
            "WHERE series_id = %s AND observation_date = %s",
            (canonical, as_of),
        )
        warehouse.execute_sql(
            "DELETE FROM derived.yoy_changes "
            "WHERE series_id = %s AND observation_date = %s",
            (canonical, as_of),
        )
        warehouse.execute_sql(
            "DELETE FROM derived.trends "
            "WHERE series_id = %s AND observation_date = %s",
            (canonical, as_of),
        )
    warehouse.execute_sql(
        "DELETE FROM regimes.regime_states "
        "WHERE as_of_date = %s "
        "  AND classifier_name = 'rule_based'",
        (as_of,),
    )


@pytest.mark.skipif(
    not os.environ.get("FRED_API_KEY"),
    reason="FRED_API_KEY not set",
)
@pytest.mark.parametrize("as_of,expected_regime", EPISODES)
def test_historical_episode(
    warehouse: TimescaleWarehouse,
    as_of: date,
    expected_regime: str,
) -> None:
    """Classify a historical episode and assert the expected regime."""
    # Fetch 400+ days before the target date so that all three transforms
    # (zscore 252-day, yoy_change 365-day lookback, trend 63-day) have
    # sufficient history at as_of.  Use as_of as end so that every fetched
    # observation satisfies observation_date <= as_of_date.
    start = as_of - timedelta(days=400)
    end = as_of

    try:
        _ingest_window(warehouse, start, end, as_of=as_of)

        ZScoreTransform(warehouse).compute(as_of)
        YoYChangeTransform(warehouse).compute(as_of)
        TrendTransform(warehouse).compute(as_of)

        clf = RuleBasedClassifier(warehouse=warehouse)
        result = clf.classify(as_of)

        assert result.regime_name == expected_regime, (
            f"Episode {as_of}: expected regime '{expected_regime}', "
            f"got '{result.regime_name}' (confidence={result.confidence}). "
            f"Regime scores: {result.rationale_detail.get('regime_scores')}. "
            f"Trigger evaluations: "
            + ", ".join(
                f"{t['variable']} {t.get('metric','')} "
                f"{t.get('op','')} {t.get('threshold','')} "
                f"= {t.get('value','?')} -> {'PASS' if t['satisfied'] else 'FAIL'}"
                for t in result.rationale_detail.get("triggers", [])
            )
        )
        assert result.confidence > Decimal("0.5"), (
            f"Episode {as_of}: confidence {result.confidence} is not > 0.5"
        )
    finally:
        _cleanup_window(warehouse, start, as_of, as_of)
