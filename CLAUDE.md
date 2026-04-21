# Markets Pipeline

A comprehensive data pipeline that operationalizes a framework for global markets. Ingests free-tier macro and market data, stores with point-in-time vintage tracking, derives analytical views, and classifies the current market regime. Designed to extend toward regime forecasting, shock simulation, and causal-relationship discovery.

## Stack

- Python 3.11+, Pydantic v2
- Apache Airflow (orchestration)
- TimescaleDB (warehouse, hypertable-keyed by `(series_id, observation_date, as_of_date)`)
- Parquet on local disk (immutable raw landing zone)
- DuckDB (analytical query layer over Parquet for research and backtesting)
- Docker Compose for local dev
- pytest + hypothesis for testing

## Layout

Monorepo with two internal packages:

- `packages/markets-core/` — pure domain model and abstract interfaces. No I/O. Depended on by everything.
- `packages/markets-pipeline/` — concrete implementations (Sources, Stores, Transforms, Classifiers) and Airflow DAGs.

## Where to read before starting work

Read only what's relevant to your task. If unsure which applies, ask first.

- **Architecture and pipeline stages:** @docs/ARCHITECTURE.md
- **Schemas, point-in-time semantics, storage tiers:** @docs/DATA-MODEL.md
- **The markets framework (matrix, tiers, coupling graph):** @docs/ONTOLOGY.md
- **Code conventions (SOLID, interfaces, error handling):** @docs/CONVENTIONS.md
- **Testing patterns and contract tests:** @docs/TESTING.md
- **Local dev setup and operations:** @docs/OPERATIONS.md
- **Planned stages 5-7 (forecasting, simulation, reasoning):** @docs/ROADMAP.md

## Non-negotiable rules

These exist because violating them silently corrupts data or breaks reproducibility. They are not style preferences.

- **Never commit `.env` files or secrets.** API keys live only in `.env` (gitignored) or environment variables.
- **Raw storage is append-only.** Parquet files in `data/raw/` are immutable once written. Never modify; write a new vintage instead.
- **Every observation carries both `observation_date` and `as_of_date`.** Point-in-time discipline is the foundation of honest backtesting. Dropping `as_of_date` is forbidden.
- **Sources must pass the contract test suite.** New `Source` implementations are not merged until the shared contract tests pass against them.
- **Protocols over concretes at boundaries.** Pipeline code depends on `Source`, `RawStore`, `Warehouse`, `Transform`, `Classifier` protocols — never on concrete class names.
- **Idempotent tasks.** Every Airflow task must be safely re-runnable for any logical date. Use `data_interval_start` / `data_interval_end`, never `datetime.now()`.
- **Never train ML models on FRED data.** FRED's ToS prohibits use "in connection with the development or training of any software program or system or machine learning." This pipeline is for decision support.

## Commands

```
make up             # bring up local stack (Timescale, Airflow)
make down           # tear down local stack
make test           # run all tests
make test-contract  # run contract tests only
make lint           # ruff + mypy strict
make backfill       # historical backfill (see OPERATIONS.md)
```

## Style

- **Type hints required.** mypy strict mode. No untyped `def`.
- **Formatting is automated.** `ruff format` runs on save and in pre-commit. Don't argue with it.
- **Pydantic for all domain models.** Frozen by default. Validate at construction.
- **Decimal, not float, for financial values.** See @docs/CONVENTIONS.md for rationale.
- **Structured logging.** Use the configured logger; never `print()` outside DAGs.

## Working with this codebase

- **Plan before large changes.** For anything spanning multiple files or packages, write a short plan and get it reviewed before implementing.
- **Ask before inventing a new abstraction.** The existing protocols (`Source`, `Store`, `Transform`, `Classifier`) cover most needs. New abstractions need justification.
- **Extend, don't rewrite.** When adding a new data source, implement the `Source` protocol. Don't restructure the ingest layer to accommodate a single provider's quirks.
- **Contract tests are not optional.** Any new implementation of a core protocol must be accompanied by the contract test suite passing against it.
- **When in doubt about the matrix ontology, check the DB.** The framework (variables, columns, rows, couplings, regimes) is data in the `ontology` schema, not hardcoded. Query it.

## Domain vocabulary

Precise terms used throughout the code and docs. Using them correctly helps Claude reason.

- **Observation:** a single data point — `(series_id, observation_date, as_of_date, value)`.
- **Vintage / as-of date:** when a value was *known*, distinct from what it's *about*.
- **Tier 2 cluster:** US risk-free rate, USD, oil, inflation, sovereign credit — the tightly-coupled core.
- **Coupling:** a directed edge between variables with regime-conditional strength.
- **Regime:** a named state of the market system (e.g., `supply_shock`, `fiscal_dominance`).
- **Phase transition:** abrupt regime change triggered by crossing a threshold in the coupling network.
