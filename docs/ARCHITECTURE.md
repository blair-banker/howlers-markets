# Architecture

This document describes the pipeline's overall design: the seven stages, the storage topology, the repository layout, and the orchestration model. Read this before touching the DAGs or adding a new stage.

## Design principles

These are load-bearing. When in doubt, return to these.

1. **Append-only raw storage.** Raw data is never modified once written. This is what makes the pipeline replayable and makes backtests honest.
2. **Point-in-time by default.** Every observation carries `observation_date` (what the data is about) and `as_of_date` (when it was known). Classifiers and forecasters always read with an as-of filter.
3. **Source-agnostic downstream.** After the source boundary, no code knows where data came from. All sources produce the same canonical `Observation` shape.
4. **Idempotent tasks.** Re-running any Airflow task for the same logical date produces the same result. Dedup keys are `(source, series_id, observation_date, as_of_date)`.
5. **Protocols over concretes at boundaries.** `Source`, `RawStore`, `Warehouse`, `Transform`, `Classifier` are abstract. Concrete implementations are injected at the composition root.
6. **Framework as data.** The markets ontology (variables, couplings, regimes) is stored in tables, not hardcoded. Extending the framework is an insert, not a code change.

## The seven stages

The pipeline is organized as seven stages with clean boundaries. Stages 1-4 are in the initial build. Stages 5-7 are scaffolded but not implemented; see [`ROADMAP.md`](ROADMAP.md).

### Stage 1: Ingest

**Purpose:** Pull raw data from external providers into an immutable landing zone.

**Input:** Source API (FRED, yfinance, Treasury Direct, BLS, EIA).
**Output:** Parquet files in `data/raw/{source}/{series_id}/{as_of_date}.parquet`.

Each source is a separate Airflow DAG with its own schedule and failure isolation. A source must implement the `Source` protocol from `markets-core`. Contract tests enforce the protocol's invariants.

Raw Parquet files are partitioned by source, series_id, and as-of date so that any vintage can be reconstructed. Files are written once and never modified.

### Stage 2: Normalize

**Purpose:** Load raw Parquet into TimescaleDB in canonical schema.

**Input:** Parquet files from Stage 1.
**Output:** Rows in the `observations` hypertable.

Type normalization, timezone alignment, and validation happen here. Rows are upserted on `(source, native_id, observation_date, as_of_date)` so re-running the stage is safe.

This is where the raw landing zone becomes queryable through standard SQL. The hypertable is chunked on `observation_date` with an index on `(series_id, observation_date, as_of_date DESC)` to make "latest-known-as-of-X" queries efficient.

### Stage 3: Derive

**Purpose:** Compute analytical views from observations.

**Input:** Observations from Stage 2, framework ontology from `ontology` schema.
**Output:** Rows in derived-view tables (`derived.zscores`, `derived.correlations`, `derived.term_structure`, etc.).

Derived values include:
- Rolling z-scores (3-month and 12-month windows)
- Rolling realized volatility
- Pairwise rolling correlations (especially within the Tier 2 cluster)
- Term-structure spreads (2s10s, 5s30s, 10y-2y)
- Credit spreads (IG OAS, HY OAS)
- Breakevens and real rates from TIPS

Each derived view is computed by a `Transform` implementation. Transforms are pure functions of observations plus configuration; they are safely re-runnable.

### Stage 4: Classify

**Purpose:** Produce point-in-time regime classifications.

**Input:** Derived views from Stage 3, framework ontology (regime definitions) from `ontology` schema.
**Output:** Rows in `regime_states` hypertable.

The initial classifier is rule-based: each regime has a set of trigger conditions on derived variables (e.g., `supply_shock` triggers when oil z-score > 2 AND headline CPI YoY > 3.5% AND core-headline spread is widening). The classifier outputs `(as_of_date, regime_name, confidence, trigger_variables, rationale)`.

A later `HiddenMarkovClassifier` is planned — see `ROADMAP.md`. Both conform to the same `Classifier` protocol. Multiple classifiers can coexist; their outputs are stored side-by-side and compared rather than merged.

### Stage 5: Forecast (planned)

Regime transition probabilities, variable path forecasts, conditional forecasts. See `ROADMAP.md`.

### Stage 6: Simulate (planned)

Shock-propagation Monte Carlo over the coupling graph. See `ROADMAP.md`.

### Stage 7: Reason (planned)

Grounded narrative explanations of current state and forecasts. See `ROADMAP.md`.

## Storage topology

Three storage tiers, each with a specific job. Do not try to unify them; each serves a distinct workload.

```
┌──────────────────────────────────────────────────────────┐
│  External providers (FRED, yfinance, Treasury, BLS, EIA) │
└──────────────────────────────────────────────────────────┘
                            │
                    Stage 1: Ingest
                            │
                            ▼
┌──────────────────────────────────────────────────────────┐
│  Parquet landing zone (data/raw/)                        │
│  - Immutable, append-only                                │
│  - Partitioned by source / series_id / as_of_date        │
│  - Source of truth for replay                            │
└──────────────────────────────────────────────────────────┘
            │                                    │
   Stage 2: Normalize                   Research / Backtest
            │                                    │
            ▼                                    ▼
┌──────────────────────────┐          ┌──────────────────────┐
│  TimescaleDB (warehouse) │          │  DuckDB (research)   │
│  - observations          │          │  - ad-hoc queries    │
│  - derived.*             │          │  - reads Parquet     │
│  - regime_states         │          │    directly          │
│  - ontology.*            │          │  - embedded, no      │
│  - Hypertables keyed on  │          │    server            │
│    (series_id,           │          └──────────────────────┘
│     observation_date,    │
│     as_of_date)          │
└──────────────────────────┘
            │
            ▼
┌──────────────────────────────────────────────────────────┐
│  Consumers: dashboard (Streamlit), classifier, APIs      │
└──────────────────────────────────────────────────────────┘
```

**Why all three:**
- Parquet is the replay-safe source of truth. If TimescaleDB is lost, everything can be rebuilt.
- TimescaleDB handles production queries: point-in-time reads, joins with ontology, writes from the classifier, serving the dashboard.
- DuckDB handles research: heavy correlation matrices, backtests across years, ad-hoc exploration. It queries the Parquet files directly with no data movement.

Full schema details are in [`DATA-MODEL.md`](DATA-MODEL.md).

## Repository layout

```
markets-pipeline/
├── packages/
│   ├── markets-core/                     # Pure domain + interfaces
│   │   ├── src/markets_core/
│   │   │   ├── domain/                   # Pydantic models
│   │   │   │   ├── observation.py
│   │   │   │   ├── series.py
│   │   │   │   ├── regime.py
│   │   │   │   └── ontology.py           # Variable, Coupling, MatrixCell
│   │   │   ├── interfaces/               # Protocols
│   │   │   │   ├── source.py
│   │   │   │   ├── store.py
│   │   │   │   ├── transform.py
│   │   │   │   └── classifier.py
│   │   │   └── errors.py
│   │   └── tests/
│   │       ├── unit/                     # Domain object tests
│   │       └── contract/                 # Shared suites every impl must pass
│   │
│   └── markets-pipeline/                 # Concrete impls + Airflow
│       ├── src/markets_pipeline/
│       │   ├── sources/                  # Source implementations
│       │   │   ├── fred.py
│       │   │   ├── yfinance.py
│       │   │   └── treasury.py
│       │   ├── stores/                   # Store implementations
│       │   │   ├── parquet.py
│       │   │   └── timescale.py
│       │   ├── transforms/               # Transform implementations
│       │   ├── classifiers/              # Classifier implementations
│       │   ├── dags/                     # Airflow DAG definitions
│       │   │   ├── ingest_fred.py
│       │   │   ├── ingest_yfinance.py
│       │   │   ├── normalize.py
│       │   │   ├── derive.py
│       │   │   └── classify.py
│       │   └── config/
│       │       └── sources.yaml
│       └── tests/
│
├── infrastructure/
│   ├── docker/
│   │   ├── docker-compose.yml            # Timescale, Airflow, dashboard
│   │   └── Dockerfile.airflow
│   └── sql/
│       ├── migrations/                   # Alembic migrations for warehouse
│       └── seed/
│           └── ontology.sql              # The matrix as data
│
├── docs/                                 # Reference docs (this directory)
├── scripts/
│   ├── backfill.py
│   └── healthcheck.py
├── CLAUDE.md
├── README.md
└── pyproject.toml                        # Workspace root
```

## Orchestration model

Airflow 2.7+ with the LocalExecutor in local dev (CeleryExecutor or KubernetesExecutor if this ever scales beyond a single machine).

**DAG organization principle:** one DAG per source, one DAG per downstream stage. Stage coupling via Airflow Datasets (2.4+), not fixed schedules.

```
ingest_fred       ──┐
ingest_yfinance   ──┼──▶ [Dataset: raw_data] ──▶ normalize
ingest_treasury   ──┘                              │
                                                   ▼
                                        [Dataset: observations]
                                                   │
                                                   ▼
                                              derive
                                                   │
                                                   ▼
                                       [Dataset: derived_views]
                                                   │
                                                   ▼
                                              classify
```

This gives each ingest DAG its own schedule (tied to the provider's release cadence where known) while downstream stages run as soon as their inputs are fresh, without fixed time-of-day assumptions.

## Configuration

Three layers:

1. **Environment variables** (secrets, host-specific): in `.env` (gitignored), loaded by Docker Compose and Python.
2. **YAML config** (series definitions, source settings): in `packages/markets-pipeline/config/`.
3. **Database-stored config** (the ontology, regime definitions): in the `ontology` schema, seeded from `infrastructure/sql/seed/ontology.sql`.

The boundary: static, per-deployment things are env vars or YAML; things users of the pipeline might want to extend at runtime (new variables, new regimes, new couplings) are in the DB.

## Adding a new data source

High-level recipe (full protocol details are in [`CONVENTIONS.md`](CONVENTIONS.md)):

1. Add a new `Source` implementation under `packages/markets-pipeline/src/markets_pipeline/sources/`.
2. The implementation must pass the contract test suite (`packages/markets-core/tests/contract/test_source_contract.py`).
3. Add the series you want to pull to `packages/markets-pipeline/config/sources.yaml`.
4. Add a new Airflow DAG in `packages/markets-pipeline/src/markets_pipeline/dags/` for the source's ingest schedule.
5. Update the ontology if the new series represents a new variable: insert a row into `ontology.variables` via a SQL migration.

No other stage needs to change. That's the point of the source-agnostic downstream.

## Adding a new derived view

1. Add a `Transform` implementation under `packages/markets-pipeline/src/markets_pipeline/transforms/`.
2. Pass the contract test suite for `Transform`.
3. Add a new table under the `derived` schema via Alembic migration.
4. Wire the transform into the `derive` DAG.

## Adding a new classifier

1. Add a `Classifier` implementation under `packages/markets-pipeline/src/markets_pipeline/classifiers/`.
2. Pass the contract test suite for `Classifier`.
3. Optionally register the classifier in `ontology.classifiers` so the dashboard can show its outputs alongside existing ones.
4. The `classify` DAG can run multiple classifiers; add yours to the DAG's classifier list.
