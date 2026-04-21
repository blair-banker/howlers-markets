# Conventions

This document describes code-level patterns specific to this project that aren't enforced by the linter. The ruff and mypy configurations handle mechanical rules; this file handles architectural rules.

If a rule is enforceable by a tool, it belongs in the tool, not here. If Claude Code needs to be told a rule at runtime, it belongs here.

## SOLID applied to this project

### Single Responsibility

Each class does one thing. In this codebase:

- A `Source` fetches data from one provider. It does not transform, store, or classify.
- A `Transform` computes one derived view. It does not fetch, store, or chain to another transform.
- A `Classifier` assigns a regime label. It does not fetch, transform, or store — it reads from the warehouse and writes to `regime_states`.
- A `Store` persists and reads data. It does not fetch from sources or compute derived values.

If a class you are writing has two reasons to change (e.g., "the FRED API changes" AND "our storage format changes"), split it.

### Open/Closed

Adding a new data source must never require modifying existing code. This is what the `Source` protocol buys.

- To add FRED: implement `Source`, done. No changes to the Normalize stage, the DAG runner, or the ontology.
- To add a new derived view: implement `Transform`, done. No changes to the derive DAG's framework.
- To add a new classifier: implement `Classifier`, done. No changes to existing classifiers' outputs.

If you find yourself modifying `normalize.py` to accommodate a source-specific quirk, back up. The quirk belongs inside the source implementation, not in downstream code.

### Liskov Substitution

Every `Source` returns the canonical `Observation` shape. Every `Store` accepts it. Every `Transform` produces its documented output type regardless of which concrete implementation is used.

Source-specific quirks (FRED's vintage-date semantics, yfinance's OHLC tuple, Treasury's XML structure) are normalized *inside* the source boundary. Downstream code must not be able to tell where an observation came from.

**Violation smell:** if you're adding an `isinstance` check on a source type in downstream code, you have a Liskov violation. The fix is almost always to push the normalization into the source.

### Interface Segregation

Don't combine unrelated concerns into one protocol. Separate protocols for `Fetcher`, `Validator`, `Normalizer` even if a source often implements all three. A `Transform` consumer should depend on the smallest interface it needs.

In practice this means: our protocols are narrow. `Source` has `fetch`, `health_check`, `metadata` — not `transform` or `classify`. Resist the temptation to add methods "because they might be useful."

### Dependency Inversion

Pipeline code depends on abstractions (the protocols in `markets-core`), not on concrete implementations (`FredSource`, `TimescaleWarehouse`). Concrete implementations are injected at the composition root — typically an Airflow DAG's top-level construction.

```python
# Correct: DAG constructs concrete implementations and injects them
def build_ingest_dag() -> DAG:
    source: Source = FredSource(api_key=os.environ["FRED_API_KEY"])
    store: RawStore = ParquetRawStore(path=RAW_PATH)
    runner = IngestRunner(source=source, store=store)  # runner depends on Protocols
    ...

# Incorrect: runner hardcodes the concrete class
class IngestRunner:
    def __init__(self) -> None:
        self.source = FredSource(...)  # coupled to a specific source
```

The benefit shows up at test time. `IngestRunner` can be unit-tested with a `FakeSource` and `FakeStore` that conform to the protocols — no network, no disk, fast.

## Protocols and implementations

Use `typing.Protocol` (runtime-checkable when needed) rather than ABCs for interfaces. Pydantic models for domain objects.

```python
from typing import Protocol, runtime_checkable

@runtime_checkable
class Source(Protocol):
    """Read-only data source. Implementations must be idempotent and side-effect-free."""

    source_name: str

    def fetch(
        self,
        series_id: SeriesId,
        start: date,
        end: date,
        as_of: date | None = None,
    ) -> Iterator[Observation]:
        """Fetch observations. Streams for memory safety on large ranges."""
        ...

    def health_check(self) -> HealthStatus:
        """Verify reachability and credentials. No data fetched."""
        ...

    def metadata(self, series_id: SeriesId) -> SeriesMetadata:
        """Describe a series."""
        ...
```

Concrete implementations live in `packages/markets-pipeline/src/markets_pipeline/sources/`. They never define their own interface; they conform to the `Source` protocol from `markets-core`.

## Error handling

### Exception hierarchy

All pipeline exceptions descend from `MarketsCoreError`. Concrete types:

```
MarketsCoreError
├── ValidationError            # Domain invariant violated
├── DataSourceError            # Source-side failure
│   └── SeriesNotFoundError    # Requested series doesn't exist
├── StorageError               # Store read/write failed
└── ConfigurationError         # Invalid or missing config
```

Callers can catch the base class for uniform handling ("any pipeline error → retry") or specific types for targeted handling ("series not found → skip and warn").

### Wrapping provider exceptions

Providers raise their own exception types. Wrap them at the source boundary:

```python
class FredSource:
    def fetch(self, series_id: SeriesId, ...) -> Iterator[Observation]:
        try:
            raw = self._client.get_series(series_id.native_id, ...)
        except requests.HTTPError as e:
            if e.response.status_code == 404:
                raise SeriesNotFoundError(
                    f"Series {series_id} not found at FRED",
                    source="fred",
                    series_id=str(series_id),
                ) from e
            raise DataSourceError(
                f"FRED request failed: {e}",
                source="fred",
                series_id=str(series_id),
            ) from e
        ...
```

**Never swallow exceptions silently.** If you catch an exception and continue, log it at WARNING or higher, with structured fields identifying the source and series. Silent failures in a data pipeline are the worst class of bug — results look correct, but they aren't.

### Retry policy

Transient failures (network timeouts, rate limits, 5xx errors) are retried automatically by Airflow with exponential backoff. Do not write your own retry loops inside source code — you'll interact badly with Airflow's retry semantics.

Permanent failures (404s, schema errors, validation failures) are not retried. Raising `SeriesNotFoundError` tells the DAG "this will not succeed no matter how many times you try."

## Decimal vs float

Use `Decimal` for financial values. This is not negotiable.

**Why:**
- `0.1 + 0.2 != 0.3` in IEEE 754 float. For z-score thresholds, trigger conditions, and ratios this matters.
- Serialization to Parquet and Postgres round-trips exactly.
- Explicit `None` for missing, instead of `float('nan')` which silently propagates and compares false to itself.

**How:** All Pydantic models have `value: Decimal | None`. When computing, cast to `Decimal` from strings or integers, not from floats (`Decimal('1.1')`, not `Decimal(1.1)`). For numpy/pandas operations where float is required for performance, convert at the computation boundary and convert back to Decimal before persisting.

## Configuration

Three tiers, in order of binding priority (later overrides earlier):

1. **YAML config** (`packages/markets-pipeline/config/*.yaml`) — defaults, committed to the repo.
2. **Environment variables** (`.env`, gitignored) — secrets and host-specific settings.
3. **Command-line arguments** — rare overrides for ad-hoc runs.

Config is loaded into typed Pydantic settings objects, never read from `os.environ` directly in business logic. This keeps the code testable.

```python
class PipelineSettings(BaseSettings):
    model_config = SettingsConfigDict(env_prefix="MARKETS_")

    fred_api_key: SecretStr
    postgres_dsn: PostgresDsn
    raw_storage_path: DirectoryPath
    log_level: Literal["DEBUG", "INFO", "WARNING", "ERROR"] = "INFO"
```

## Logging

Use structured logging. Every log line has context fields that make filtering and correlation possible.

```python
logger.info(
    "ingest_completed",
    source=self.source_name,
    series_id=str(series_id),
    observations=len(results),
    duration_seconds=elapsed,
)
```

**Rules:**
- Never use `print()` outside of Airflow DAGs (and even there, prefer `logger.info`).
- Log at INFO for routine successes, WARNING for handled-but-notable events, ERROR for exceptions, DEBUG for diagnostics.
- Include the series_id or source name in every log line where relevant. Grepping logs for one series should return all events for that series.
- Do not log secrets. SecretStr values render as `**********`.

## Idempotency

Every pipeline task must be safely re-runnable for the same logical date. This is what makes replay, backfill, and crash recovery work.

**Patterns:**

- **Ingest:** writes Parquet files whose names include the as-of date. Re-running for the same date writes the same file (overwrite is fine if the data hasn't changed; a later as-of date produces a new file).
- **Normalize:** upserts into the observations table with conflict key `(series_id, observation_date, as_of_date)`. Re-running for the same logical date is a no-op if nothing changed.
- **Derive:** recomputes the derived view for the logical date; deletes and re-inserts the rows for that date.
- **Classify:** writes `regime_states` keyed on `(as_of_date, classifier_name, classifier_version)`. Upserts.

**Anti-pattern:** using `datetime.now()` anywhere in task logic. The task's concept of "now" is the DAG's logical date (`data_interval_start` / `data_interval_end`), not wall-clock time. Using wall-clock time breaks backfill.

## Testing patterns

Full details in [`TESTING.md`](TESTING.md). The short version:

- **Unit tests** for domain objects and pure functions. Fast, no I/O.
- **Contract tests** for every implementation of a core protocol. Shared test suite; every `Source` runs the same `test_source_contract.py`.
- **Integration tests** for source-to-store flows, marked `@pytest.mark.integration` so they can be excluded in normal runs.
- **Migration tests** that apply migrations to a fresh DB and verify the schema.

Run `make test` for everything, `make test-contract` for just contract tests, `make test-integration` for tests that require external services.

## Dependency versions

Pin major versions in `pyproject.toml`. Lockfile (`uv.lock` or `pip-compile`) pins exact versions. Update dependencies deliberately, not accidentally.

Critical dependencies and their version policies:

- `pydantic` — pin to v2.x. The v1→v2 migration was breaking; don't risk it.
- `airflow` — pin to the tested minor version. Airflow has non-trivial breakage between minors.
- `psycopg` — v3 (not v2). Async support and better types.
- `timescaledb` — matches Postgres version; upgrade together.

## File organization

- **One class per file** for protocols and domain objects. A file named `source.py` contains exactly the `Source` protocol and its immediate helpers.
- **Feature-grouped for implementations.** `sources/fred.py` contains the `FredSource` class and its private helpers.
- **Tests mirror source structure.** `tests/unit/domain/test_observation.py` tests `domain/observation.py`.
- **No cross-package imports within implementations.** `markets_pipeline.sources.fred` does not import from `markets_pipeline.stores.timescale`. Cross-boundary communication happens via the core protocols.

## Naming

- **Classes:** PascalCase, descriptive: `FredSource`, `TimescaleWarehouse`, `RuleBasedClassifier`.
- **Protocols:** the abstract name without qualification: `Source`, `Store`, `Transform`. Concrete classes qualify with their implementation: `FredSource implements Source`.
- **Variables/functions:** snake_case. Boolean variables start with `is_`, `has_`, or `should_`.
- **Module-level constants:** UPPER_SNAKE_CASE.
- **Private helpers:** leading underscore. A leading underscore means "not part of the public API"; respect it.

## Commit discipline

- Small, focused commits. One commit per file for docs changes; one commit per logical change for code.
- Imperative mood in commit messages: "add FRED source" not "added" or "adds."
- Reference issue numbers where applicable: "add FRED source (#12)."
- Never commit `.env`, generated files, or `data/`.

See [`TESTING.md`](TESTING.md) for what tests must pass before commit.
