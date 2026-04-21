# Testing

This document describes the testing strategy: what we test, how we test it, and where each kind of test lives. Contract tests are the most important pattern here and the one most likely to be skipped or done incorrectly without explicit guidance.

## The test pyramid

We organize tests into four layers, broad at the bottom and narrow at the top.

```
                    ┌──────────────────────┐
                    │   E2E / Smoke Tests  │   Very few; end-to-end DAG runs
                    └──────────────────────┘
              ┌────────────────────────────────┐
              │      Integration Tests         │   Source → Store, DB migrations
              └────────────────────────────────┘
        ┌──────────────────────────────────────────┐
        │           Contract Tests                 │   One suite per protocol
        └──────────────────────────────────────────┘
   ┌────────────────────────────────────────────────────┐
   │                  Unit Tests                        │   Domain objects, pure logic
   └────────────────────────────────────────────────────┘
```

Run the pyramid from the bottom: unit tests in milliseconds, contract tests in seconds, integration tests in tens of seconds, E2E tests as needed.

## Unit tests

**What:** domain objects, pure functions, error hierarchies, configuration parsing.

**Characteristics:** fast (<100ms each), deterministic, no I/O, no network, no Docker.

**Location:** `packages/*/tests/unit/`, mirroring the source structure.

**Tools:** `pytest`, `hypothesis` for property-based tests on invariants.

**Example patterns:**

```python
def test_observation_rejects_observation_after_as_of():
    with pytest.raises(ValidationError):
        Observation(
            series_id=SeriesId(source="fred", native_id="DGS10"),
            observation_date=date(2026, 4, 15),
            as_of_date=date(2026, 4, 10),  # before observation_date — invalid
            value=Decimal("4.31"),
        )


@given(obs_date=dates(min_value=date(2000, 1, 1), max_value=date(2030, 12, 31)),
       days_later=integers(min_value=0, max_value=3650))
def test_observation_accepts_any_as_of_at_or_after_observation(obs_date, days_later):
    as_of = obs_date + timedelta(days=days_later)
    obs = Observation(
        series_id=SeriesId(source="test", native_id="X"),
        observation_date=obs_date,
        as_of_date=as_of,
        value=Decimal("1.0"),
    )
    assert obs.as_of_date >= obs.observation_date
```

Prefer Hypothesis for invariants that should hold across a wide input space. Prefer parametrized pytest fixtures for specific cases with known expected outputs.

## Contract tests

**This is the most important pattern in the codebase. Read this section twice.**

When multiple concrete implementations must conform to the same protocol, we define a *shared test suite* that runs against every implementation. Any implementation that doesn't pass the contract is, by definition, not a valid implementation of the protocol.

**Why:** without contract tests, protocols are just documentation. Nothing enforces that `FredSource` and `YfinanceSource` behave the same way at their boundaries. The first time downstream code fails because of a subtle behavioral difference, the abstraction has already paid a cost.

**Location:** `packages/markets-core/tests/contract/`. The test suites live *with the protocol*, not with the implementations, because the contract belongs to the protocol.

**Structure:**

```python
# packages/markets-core/tests/contract/test_source_contract.py

class SourceContract:
    """Shared contract every Source implementation must satisfy.

    Subclass this in the implementation's test file and override
    `source_fixture` to return your concrete implementation.
    """

    @pytest.fixture
    def source(self) -> Source:
        raise NotImplementedError("Override in subclass")

    @pytest.fixture
    def known_series_id(self) -> SeriesId:
        raise NotImplementedError("Override in subclass")

    def test_fetch_returns_iterator(self, source, known_series_id):
        result = source.fetch(known_series_id, date(2026, 1, 1), date(2026, 1, 31))
        assert hasattr(result, "__iter__")

    def test_fetch_observations_have_valid_dates(self, source, known_series_id):
        for obs in source.fetch(known_series_id, date(2026, 1, 1), date(2026, 1, 31)):
            assert obs.observation_date <= obs.as_of_date
            assert obs.observation_date >= date(2026, 1, 1)

    def test_fetch_missing_series_raises(self, source):
        fake_id = SeriesId(source=source.source_name, native_id="DEFINITELY_NOT_A_REAL_SERIES_XYZ")
        with pytest.raises(SeriesNotFoundError):
            list(source.fetch(fake_id, date(2026, 1, 1), date(2026, 1, 31)))

    def test_health_check_returns_status(self, source):
        status = source.health_check()
        assert status.healthy in (True, False)  # not the point; just structure

    def test_metadata_raises_for_missing_series(self, source):
        fake_id = SeriesId(source=source.source_name, native_id="DEFINITELY_NOT_A_REAL_SERIES_XYZ")
        with pytest.raises(SeriesNotFoundError):
            source.metadata(fake_id)
```

**Implementation-side usage:**

```python
# packages/markets-pipeline/tests/sources/test_fred_source.py

class TestFredSourceContract(SourceContract):
    """FredSource must satisfy the Source contract."""

    @pytest.fixture
    def source(self) -> Source:
        return FredSource(api_key=os.environ["FRED_API_KEY"])

    @pytest.fixture
    def known_series_id(self) -> SeriesId:
        return SeriesId(source="fred", native_id="DGS10")


class TestFredSourceSpecific:
    """Tests specific to FredSource, not from the contract."""

    def test_handles_alfred_vintage_correctly(self):
        # Tests behavior unique to FRED (ALFRED revision tracking)
        ...
```

Contract tests typically require live external services (FRED API, yfinance backend). Mark them `@pytest.mark.integration` so they can be skipped in fast test runs:

```python
@pytest.mark.integration
@pytest.mark.requires_fred_key
class TestFredSourceContract(SourceContract):
    ...
```

**Contract test rules:**

1. **Every protocol has a contract suite.** `Source`, `Store`, `Transform`, `Classifier` each have one.
2. **Every implementation subclasses it.** No exceptions. If you're merging a new `Source`, the contract tests pass or the PR doesn't merge.
3. **Contract tests assert behavior, not implementation.** They check what the protocol promises, not how it's achieved.
4. **New contract assertions propagate.** When we discover a new invariant the protocol should enforce, we add it to the contract suite — and any existing implementation that now fails needs to be fixed.

## Integration tests

**What:** tests that cross component boundaries — Source reading from a live API, Store writing to a real Postgres, DAG orchestration.

**Characteristics:** slower (seconds), require infrastructure (Docker Compose, API keys), can fail for environmental reasons.

**Location:** `packages/markets-pipeline/tests/integration/`.

**Markers:** always `@pytest.mark.integration`, often also `@pytest.mark.requires_fred_key` etc.

**Example:**

```python
@pytest.mark.integration
def test_fred_to_parquet_roundtrip(tmp_path):
    """A full Stage 1 run should produce queryable Parquet output."""
    source = FredSource(api_key=os.environ["FRED_API_KEY"])
    store = ParquetRawStore(path=tmp_path)

    observations = list(source.fetch(
        SeriesId(source="fred", native_id="DGS10"),
        start=date(2026, 3, 1),
        end=date(2026, 3, 31),
    ))
    store.append("fred", SeriesId(source="fred", native_id="DGS10"), observations,
                 as_of=datetime(2026, 4, 1, tzinfo=UTC))

    readback = list(store.read("fred", SeriesId(source="fred", native_id="DGS10")))
    assert readback == observations
```

**Rule:** integration tests must be idempotent. Running them twice should produce the same result. Clean up resources in teardown.

## Migration tests

Schema migrations are tested by applying them to a fresh database and verifying the resulting schema:

```python
@pytest.mark.integration
def test_all_migrations_apply_to_empty_db(fresh_postgres):
    """Every migration, in order, applies cleanly to an empty database."""
    alembic_upgrade(fresh_postgres.url, revision="head")
    assert table_exists(fresh_postgres, "observations.observations")
    assert table_exists(fresh_postgres, "ontology.variables")
    # ... etc.


@pytest.mark.integration
def test_migrations_are_reversible(fresh_postgres):
    """Every migration has a working downgrade."""
    alembic_upgrade(fresh_postgres.url, revision="head")
    alembic_downgrade(fresh_postgres.url, revision="base")
    assert not table_exists(fresh_postgres, "observations.observations")
```

## E2E / Smoke tests

Minimal — enough to confirm that the full pipeline wires up correctly. A single DAG run with one series, end to end, verified by reading from the warehouse.

Run these manually or in CI on merges to main, not on every commit. They're slow and they verify coordination, not logic.

## Test data fixtures

Fixtures for synthetic observations, synthetic ontology, and synthetic regime states live in `packages/markets-core/tests/fixtures/`. Use them in unit and contract tests to avoid duplicating setup.

```python
@pytest.fixture
def synthetic_tier2_observations() -> list[Observation]:
    """Ten business days of fake Tier 2 cluster data."""
    ...
```

**Rule:** synthetic data should look *unrealistic* enough that no one mistakes it for real data. Use round numbers, obvious patterns, and series IDs like `test:FAKE_RATE`.

## Coverage

Target: 80%+ line coverage for `markets-core`, 70%+ for `markets-pipeline`. Lower for Airflow DAGs (hard to test in isolation).

**Rules:**

- 100% coverage for domain objects and the exception hierarchy. These are simple and the bar is high.
- Contract test passage is more important than line coverage for implementations. An implementation with 90% coverage but failing a contract assertion is worse than one with 70% coverage that passes.
- Don't chase coverage by testing trivial code. Getter-setter tests are noise.

## Running tests

```bash
make test                          # all tests except integration/slow
make test-contract                 # contract tests only
make test-integration              # integration tests (requires Docker up)
make test-all                      # everything including slow
make test-cov                      # with coverage report

pytest -k "test_observation"       # subset matching name
pytest -m "not slow and not integration"  # exclude markers
pytest --hypothesis-seed=12345     # reproducible property tests
```

## Pre-commit checks

Every commit must pass:

1. `ruff format --check` — formatting is consistent
2. `ruff check` — linting passes
3. `mypy --strict` — type-checks cleanly
4. `pytest -m "not integration"` — unit and contract tests pass

Integration tests run in CI, not in pre-commit (they require Docker and API keys).

## Debugging failing tests

**If a contract test fails for a new implementation:** the implementation doesn't correctly implement the protocol. Fix the implementation, not the contract.

**If a contract test fails for an existing implementation after a protocol change:** the protocol change is a breaking change. Either update all implementations or revert.

**If an integration test fails intermittently:** flakiness usually means the test depends on state it doesn't set up. Look for shared fixtures, time-of-day dependencies, or order-sensitive test execution.

**If a unit test fails with a cryptic Pydantic error:** check that the domain object's validators are ordered correctly. Pydantic v2 evaluates validators in definition order; a `@model_validator(mode="after")` that depends on a field validator must come later in the class body.

## Testing philosophy

Tests are load-bearing. They define what "correct" means for code that's too subtle to verify by inspection. Contract tests specifically encode the behavior we expect from any plausible implementation, so they act as executable specifications.

Write the test first when you know what the code must do but aren't sure how. Write the implementation first when the approach is obvious and you're not sure what to test. Both are legitimate; choose based on what's actually uncertain.

When a test fails in a way that surprises you, the test has done its job. Don't delete it or make it easier to pass. Figure out what assumption was wrong.
