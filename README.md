# Markets Pipeline

A data pipeline for the global-markets framework: ingest, store, classify, and eventually forecast market regimes using free public data sources.

## Why

Understanding global markets requires tracking a tightly-coupled set of macro and market variables and recognizing which *regime* the system is currently in. This project operationalizes a framework for that — as a living, queryable, extensible pipeline — starting with the foundational layers (ingest, storage, derivation, classification) and extending toward forecasting and causal analysis.

## What it does today

- **Ingests** ~30 curated series from FRED, yfinance, U.S. Treasury Direct, BLS, and EIA.
- **Stores** every observation with full point-in-time vintage tracking (what was known when).
- **Derives** z-scores, rolling correlations, term structures, credit spreads.
- **Classifies** the current regime against a rule-based definition grounded in the framework.

## What's planned

- Regime *forecasting* (transition probabilities, variable path forecasts)
- Shock *simulation* (conditional forecasts, propagation through the coupling graph)
- Causal-relationship *discovery* (Granger tests, transfer entropy, regime-conditional VAR)
- Narrative *reasoning* (grounded explanations of what's moving and why)

See [`docs/ROADMAP.md`](docs/ROADMAP.md) for details.

## Architecture at a glance

```
┌─────────────┐    ┌─────────────┐    ┌──────────────┐    ┌─────────────┐
│   Sources   │───▶│ Raw Parquet │───▶│ TimescaleDB  │───▶│  Classifier │
│ (FRED, etc) │    │ (immutable) │    │ (warehouse)  │    │             │
└─────────────┘    └─────────────┘    └──────────────┘    └─────────────┘
                          │                   │
                          ▼                   ▼
                   ┌─────────────┐    ┌─────────────┐
                   │   DuckDB    │    │  Dashboard  │
                   │  (research) │    │ (Streamlit) │
                   └─────────────┘    └─────────────┘
```

The framework ontology (variables, couplings, regimes) lives in the warehouse as data, not as hardcoded config. See [`docs/ONTOLOGY.md`](docs/ONTOLOGY.md).

## Quick start

```bash
# Prerequisites: Docker, Docker Compose, Python 3.11+

# 1. Register for a free FRED API key: https://fredaccount.stlouisfed.org/apikeys

# 2. Configure environment
cp .env.example .env
# edit .env — at minimum, set FRED_API_KEY

# 3. Bring up the stack
make up

# 4. Seed ontology tables (the matrix as data)
make seed-ontology

# 5. Run initial backfill
make backfill START=2020-01-01

# 6. Open Airflow UI: http://localhost:8080  (admin/admin)
```

Full setup instructions in [`docs/OPERATIONS.md`](docs/OPERATIONS.md).

## Repository layout

```
markets-pipeline/
├── packages/
│   ├── markets-core/         # Pure domain model and abstract interfaces
│   └── markets-pipeline/     # Concrete implementations and Airflow DAGs
├── infrastructure/
│   ├── docker/               # Dockerfiles and docker-compose.yml
│   └── sql/                  # TimescaleDB migrations and seed data
├── docs/                     # Reference documentation for humans and Claude Code
├── scripts/                  # Utility scripts (backfill, health-check)
├── CLAUDE.md                 # Entry point for Claude Code sessions
└── README.md                 # You are here
```

## Documentation

| Document | Purpose |
|----------|---------|
| [`CLAUDE.md`](CLAUDE.md) | Persistent context for Claude Code |
| [`docs/ARCHITECTURE.md`](docs/ARCHITECTURE.md) | The seven-stage pipeline and its design |
| [`docs/DATA-MODEL.md`](docs/DATA-MODEL.md) | Schemas and point-in-time semantics |
| [`docs/ONTOLOGY.md`](docs/ONTOLOGY.md) | The markets framework (matrix, tiers, couplings) |
| [`docs/CONVENTIONS.md`](docs/CONVENTIONS.md) | Code conventions and design principles |
| [`docs/TESTING.md`](docs/TESTING.md) | Testing strategy and contract tests |
| [`docs/OPERATIONS.md`](docs/OPERATIONS.md) | Running the stack locally |
| [`docs/ROADMAP.md`](docs/ROADMAP.md) | Planned work (stages 5-7) |

## License

TBD. This is a personal project and not for commercial use. Data sources have their own terms — FRED's ToS in particular prohibits use for training machine-learning models; this pipeline is for decision support only.

## Status

Active development. Stages 1-4 of the seven-stage pipeline are the initial build target.
