# Data Model

This document describes the canonical data shapes, the point-in-time semantics that make the pipeline honest, and the schema layout in TimescaleDB. Read this before touching any stage that reads or writes data.

## Core concept: point-in-time

Economic data is revised. The March CPI value you saw on April 10, 2026 is not the same number you'll see a year later — BLS revises for seasonal factors, methodology changes, and corrections. Market data is less prone to revision but is not immune (late-print corrections, corporate actions on equity tickers).

A classifier that reads *current-revised* data and runs a backtest against *current-revised* data is fooling itself. It's using information that wasn't actually available at the time of the decision. This is the single most common way macro/market pipelines produce misleadingly optimistic results.

The fix: every observation carries two dates.

- **`observation_date`** — what the data is *about*. (e.g., `2026-03-01` for March 2026 CPI.)
- **`as_of_date`** — when this value was *known*. (e.g., `2026-04-10` for the initial release, `2026-04-25` for a revision.)

An observation is *(series, observation_date, as_of_date, value)*. A classifier running "as of date D" reads, for each series, the observation with the latest `as_of_date <= D`. This is the honest view.

For market data where revision is rare, ingest sets `as_of_date = observation_date` at write time. The schema still carries both fields for uniformity; downstream code doesn't need to branch on whether data is revisable.

## Storage tiers and their jobs

| Tier | Technology | Contents | Writer | Readers |
|------|-----------|----------|--------|---------|
| Raw | Parquet files | Unprocessed source data, partitioned by `(source, series_id, as_of_date)` | Ingest DAGs only | Normalize DAG; DuckDB (research) |
| Warehouse | TimescaleDB | Normalized observations, derived views, ontology, regime states | Normalize + Derive + Classify DAGs | Dashboard, APIs, other classifiers |
| Research | DuckDB | None of its own — queries Parquet directly | N/A | Notebooks, ad-hoc analysis, backtest scripts |

The raw tier is the source of truth. The warehouse and research tiers are derived from it and can be rebuilt.

## Parquet layout

```
data/raw/
├── fred/
│   ├── DGS10/
│   │   ├── 2020-01-01.parquet
│   │   ├── 2020-01-02.parquet
│   │   └── ...
│   ├── CPILFESL/
│   │   └── ...
├── yfinance/
│   ├── ^VIX/
│   │   └── ...
│   └── ^GSPC/
│       └── ...
└── treasury/
    └── ...
```

Path structure: `data/raw/{source}/{native_id}/{as_of_date}.parquet`. Each file contains all observations fetched at that as-of date for that series. Files are written once and never modified. A revision produces a new file with a later as-of date.

Parquet schema (same columns regardless of source):

| Column | Type | Notes |
|--------|------|-------|
| `source` | string | e.g., `"fred"` |
| `native_id` | string | Source-native identifier (e.g., `"DGS10"`) |
| `observation_date` | date | What the data is about |
| `as_of_date` | date | When it was known |
| `value` | decimal(18,8) | The numeric value; nullable |
| `ingested_at` | timestamp UTC | When the pipeline wrote this row |
| `source_revision` | string | Provider-specific revision ID if available |

## Warehouse schema

TimescaleDB is organized into four schemas:

```
public           -- default Postgres schema; nothing here except Airflow's tables
observations     -- core observation data
derived          -- computed analytical views
ontology         -- the framework as data (variables, couplings, regimes)
regimes          -- classifier outputs over time
```

### `observations` schema

The heart of the warehouse. A single hypertable.

```sql
CREATE TABLE observations.observations (
    series_id         text        NOT NULL,   -- canonical form "source:native_id"
    observation_date  date        NOT NULL,
    as_of_date        date        NOT NULL,
    value             numeric(18, 8),         -- nullable: missing/NA at this vintage
    ingested_at       timestamptz NOT NULL DEFAULT now(),
    source_revision   text,

    PRIMARY KEY (series_id, observation_date, as_of_date)
);

SELECT create_hypertable(
    'observations.observations',
    'observation_date',
    chunk_time_interval => INTERVAL '1 year'
);

CREATE INDEX observations_latest_as_of_idx
    ON observations.observations (series_id, observation_date, as_of_date DESC);
```

**Query pattern — get point-in-time view as of date D:**

```sql
SELECT DISTINCT ON (series_id, observation_date)
    series_id, observation_date, value
FROM observations.observations
WHERE as_of_date <= :as_of_date
  AND series_id = ANY(:series_ids)
ORDER BY series_id, observation_date, as_of_date DESC;
```

This pattern — `DISTINCT ON` with `ORDER BY as_of_date DESC` — is the canonical way to read point-in-time. Wrap it in a database view or a Python helper; do not hand-write this at every call site.

### `derived` schema

Each derived view is its own table. Shared shape:

```sql
CREATE TABLE derived.zscores (
    series_id         text        NOT NULL,
    observation_date  date        NOT NULL,
    as_of_date        date        NOT NULL,
    window_days       integer     NOT NULL,   -- 63 for ~3m, 252 for ~12m
    zscore            numeric(10, 4),
    computed_at       timestamptz NOT NULL DEFAULT now(),

    PRIMARY KEY (series_id, observation_date, as_of_date, window_days)
);

SELECT create_hypertable('derived.zscores', 'observation_date');
```

Similar tables for `derived.correlations` (with two series_ids and a window), `derived.term_structure`, `derived.credit_spreads`, etc. Each derived view is computed from the point-in-time view of `observations` — not from current-revised data.

### `ontology` schema

This is where the framework lives as data. These tables are reference data: small, slowly-changing, heavily-joined.

```sql
CREATE TABLE ontology.columns (
    id           serial PRIMARY KEY,
    name         text NOT NULL UNIQUE,       -- e.g., "monetary_credit"
    display_name text NOT NULL,              -- "Monetary / Credit"
    description  text,
    position     integer NOT NULL            -- for ordering the matrix
);

CREATE TABLE ontology.depth_rows (
    id           serial PRIMARY KEY,
    depth_level  integer NOT NULL UNIQUE,    -- 1..10
    name         text NOT NULL UNIQUE,       -- e.g., "price_of_money"
    display_name text NOT NULL,              -- "Price-of-money layer"
    description  text
);

CREATE TABLE ontology.variables (
    id              serial PRIMARY KEY,
    name            text NOT NULL UNIQUE,    -- canonical: "us_10y_treasury"
    display_name    text NOT NULL,           -- "US 10-Year Treasury Yield"
    description     text,
    column_id       integer NOT NULL REFERENCES ontology.columns(id),
    depth_row_id    integer NOT NULL REFERENCES ontology.depth_rows(id),
    tier            integer NOT NULL CHECK (tier IN (1, 2, 3)),
    primary_series  text,                    -- canonical series_id if exists
    created_at      timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE ontology.couplings (
    id                     serial PRIMARY KEY,
    source_variable_id     integer NOT NULL REFERENCES ontology.variables(id),
    target_variable_id     integer NOT NULL REFERENCES ontology.variables(id),
    coupling_type          text NOT NULL,    -- "reflexive", "doom_loop", "collateral", etc.
    normal_regime_strength numeric(4, 3),    -- expected strength in [0,1] in normal regimes
    stress_regime_strength numeric(4, 3),    -- expected strength in [0,1] in stress regimes
    estimation_method      text,             -- "correlation", "granger", "expert_judgment"
    rationale              text,
    created_at             timestamptz NOT NULL DEFAULT now(),

    CONSTRAINT no_self_loop CHECK (source_variable_id <> target_variable_id)
);

CREATE TABLE ontology.regimes (
    id           serial PRIMARY KEY,
    name         text NOT NULL UNIQUE,       -- "supply_shock", "fiscal_dominance", etc.
    display_name text NOT NULL,
    description  text NOT NULL,
    tier         integer NOT NULL CHECK (tier IN (1, 2, 3))
);

CREATE TABLE ontology.regime_triggers (
    id               serial PRIMARY KEY,
    regime_id        integer NOT NULL REFERENCES ontology.regimes(id),
    variable_id      integer NOT NULL REFERENCES ontology.variables(id),
    condition        text NOT NULL,          -- DSL: "zscore_3m > 2.0"
    weight           numeric(4, 3) NOT NULL, -- importance of this trigger
    description      text
);
```

Note: as of migration 002, `condition` is `jsonb`, not `text`. It holds a
validated AST with node types `compare`, `compare_spread`, `all_of`, `any_of`.
See [`docs/superpowers/specs/2026-04-22-stage-4-classifier-design.md`](superpowers/specs/2026-04-22-stage-4-classifier-design.md) §4.

The matrix columns, depth rows, Tier 2 couplings, and regime definitions are seeded via `infrastructure/sql/seed/ontology.sql` at initial deployment. Changes to the framework go through migrations, giving the ontology a version history.

### `regimes` schema

Classifier outputs over time. A hypertable keyed on classifier version so multiple classifiers can coexist.

```sql
CREATE TABLE regimes.regime_states (
    as_of_date         date        NOT NULL,
    classifier_name    text        NOT NULL,
    classifier_version text        NOT NULL,
    regime_name        text        NOT NULL REFERENCES ontology.regimes(name),
    confidence         numeric(4, 3) NOT NULL CHECK (confidence BETWEEN 0 AND 1),
    trigger_variables  text[]      NOT NULL DEFAULT ARRAY[]::text[],
    rationale          text,
    computed_at        timestamptz NOT NULL DEFAULT now(),

    PRIMARY KEY (as_of_date, classifier_name, classifier_version)
);

SELECT create_hypertable('regimes.regime_states', 'as_of_date');
```

As of migration 002, `regime_states` has two additional columns:
- `rationale_detail jsonb NOT NULL` — structured per-trigger breakdown produced by
  the classifier. Schema documented in the Stage 4 design spec §5.2.
- `ontology_version text NOT NULL` — the Alembic revision of the ontology used
  to produce this row. Two rows with the same `(classifier_version, ontology_version)`
  are guaranteed reproducible.

## Domain model (Pydantic)

The Python-side representation mirrors the SQL closely. See `packages/markets-core/src/markets_core/domain/`.

```python
class SeriesId(BaseModel):
    source: str       # lowercased at validation
    native_id: str    # source-native identifier

    def __str__(self) -> str:
        return f"{self.source}:{self.native_id}"


class Observation(BaseModel):
    model_config = ConfigDict(frozen=True)

    series_id: SeriesId
    observation_date: date
    as_of_date: date
    value: Decimal | None
    ingested_at: datetime  # UTC
    source_revision: str | None

    @model_validator(mode="after")
    def observation_not_after_as_of(self) -> Self:
        if self.observation_date > self.as_of_date:
            raise ValueError("observation_date cannot exceed as_of_date")
        return self
```

**Why `Decimal` for `value`:**
- Financial data precision matters (`0.1 + 0.2 != 0.3` in IEEE 754 float).
- Serialization round-trips are exact.
- Explicit `None` for missing rather than `float('nan')`.

The cost is minor arithmetic overhead, immaterial at our scale.

## Ingestion contract

A `Source` implementation produces `Iterator[Observation]`. The iterator:

1. Yields observations in any order; downstream does not depend on ordering.
2. Sets `as_of_date = today()` at ingest time if the source does not provide vintage information.
3. Sets `as_of_date` to the provider's vintage timestamp if it does (FRED's ALFRED provides this).
4. Raises `DataSourceError` for provider failures, `SeriesNotFoundError` if the series does not exist.

Contract tests verify these invariants for every `Source` implementation.

## Querying patterns

### The point-in-time read (most important)

Always wrapped in a helper or view. The bare pattern:

```sql
SELECT DISTINCT ON (series_id, observation_date)
    series_id, observation_date, value, as_of_date
FROM observations.observations
WHERE as_of_date <= :as_of
ORDER BY series_id, observation_date, as_of_date DESC;
```

A view `observations.v_point_in_time_latest` provides the "as of now" case; a function `observations.f_point_in_time(as_of date)` provides historical cutoffs.

### The aligned-cluster read

Given a set of series and an as-of date, return aligned daily values suitable for correlation analysis. This is a derived view, recomputed daily.

```sql
-- derived.v_cluster_daily joins Tier 2 series, forward-fills lower-frequency ones
SELECT observation_date, us_10y, dxy, brent, core_cpi_yoy, ig_oas, hy_oas
FROM derived.v_cluster_daily
WHERE observation_date BETWEEN :start AND :end;
```

### The regime timeline read

```sql
SELECT as_of_date, regime_name, confidence, trigger_variables
FROM regimes.regime_states
WHERE classifier_name = :classifier
  AND classifier_version = :version
  AND as_of_date BETWEEN :start AND :end
ORDER BY as_of_date;
```

## Migration policy

Schema changes go through Alembic migrations in `infrastructure/sql/migrations/`. Rules:

1. **Additive migrations are safe.** Adding a column, a table, or a new derived view does not require coordination.
2. **Destructive migrations require a two-step.** Deprecate a column (or table) in one release, remove it in a later release once no consumers read it.
3. **Ontology changes are migrations.** Adding a variable, coupling, or regime is a data migration in a versioned file. This gives the framework a history, just like code.
4. **Never edit historical migrations.** Add a new migration to correct a prior one.

## Backup and replay

- **Parquet raw tier** is the source of truth. Back it up (rsync, S3 sync, whatever your ops setup is).
- **TimescaleDB** can always be rebuilt from Parquet by running Normalize + Derive + Classify stages on the full history. `make rebuild-warehouse` does this.
- **Ontology seed data** is in version-controlled SQL. Rebuilt from `infrastructure/sql/seed/ontology.sql`.
- **Classifier outputs** are re-derivable from observations + ontology; they are not independently precious.
