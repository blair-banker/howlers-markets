# Operations

This document covers running the pipeline locally: initial setup, day-to-day commands, troubleshooting, and operational procedures. If something is broken and you can't figure out why, start here.

## Prerequisites

- **Docker Desktop** (or equivalent: OrbStack on macOS, Docker Engine on Linux). Compose v2.
- **Python 3.11 or 3.12**, managed via `pyenv`, `uv`, or your preferred tool.
- **Make** (standard on macOS and Linux; on Windows use WSL or install via `choco install make`).
- **A FRED API key** — free at https://fredaccount.stlouisfed.org/apikeys. Required; the pipeline will not run without it.

Optional but recommended:
- **`uv`** for fast Python package management (`pip install uv` or see https://docs.astral.sh/uv/).
- **`direnv`** for automatic env-var loading when you `cd` into the project.
- A Postgres client (`psql`, DBeaver, TablePlus) for ad-hoc queries against the warehouse.

## Initial setup

```bash
# 1. Clone the repo
git clone <repo-url> markets-pipeline
cd markets-pipeline

# 2. Configure environment
cp .env.example .env
# Open .env and fill in at minimum:
#   FRED_API_KEY=<your key>
#   POSTGRES_PASSWORD=<any strong password>

# 3. Generate Airflow Fernet key (for secrets encryption)
python -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())"
# Paste the output into .env as AIRFLOW__CORE__FERNET_KEY

# 4. Install Python packages for local development (editable installs)
make install-dev
# or manually: uv pip install -e packages/markets-core -e packages/markets-pipeline[dev]

# 5. Bring up the stack
make up

# 6. Wait for health checks to pass (~30 seconds on first run; Airflow is slow to boot)
make health

# 7. Apply database migrations
make migrate

# 8. Seed the ontology (the matrix as data)
make seed-ontology

# 9. Verify everything is healthy
make health
```

At this point you should see:
- TimescaleDB responding on `localhost:5432`
- Airflow UI at http://localhost:8080 (login: admin/admin)
- Ontology tables populated (verify with `make query-ontology`)

## Daily operations

### Starting and stopping the stack

```bash
make up        # bring everything up
make down      # stop everything (preserves data)
make nuke      # stop AND delete all data (WARNING: removes warehouse and raw data)
make logs      # tail logs from all services
make logs S=airflow-scheduler  # tail logs from one service
```

### Running ingestion

First-time backfill for a single source:

```bash
make backfill SOURCE=fred START=2020-01-01 END=2026-04-20
```

This invokes the Airflow backfill command for the `ingest_fred` DAG over the specified date range. Progress is visible in the Airflow UI.

Regular scheduled runs happen automatically once the stack is up. Check the DAG status at http://localhost:8080.

### Viewing data

Query the warehouse:

```bash
make psql                               # drop into psql connected to the warehouse
make query Q="SELECT COUNT(*) FROM observations.observations"
```

Query via DuckDB (for research over Parquet):

```bash
make duckdb                             # drop into DuckDB with views registered over raw Parquet
```

### Reloading the ontology

If you've changed `infrastructure/sql/seed/ontology.sql` (new variable, new regime, new coupling):

```bash
make seed-ontology                      # idempotent; upserts by name
```

### Rebuilding the warehouse from raw data

If the warehouse gets corrupted or a derivation bug requires recomputing:

```bash
make rebuild-warehouse                  # truncates derived + regimes, re-runs Normalize/Derive/Classify
```

This reads from raw Parquet (the source of truth) and rebuilds everything downstream. Safe to run; takes ~5-20 minutes depending on history depth.

## Troubleshooting

### Airflow UI unreachable

```bash
docker compose -f infrastructure/docker/docker-compose.yml ps
# Look for airflow-webserver — should be "running (healthy)"

# If unhealthy:
make logs S=airflow-webserver
# Common causes:
#   - Fernet key missing or invalid → regenerate and restart
#   - Metadata DB (separate Postgres) not yet ready → wait 30s and retry
#   - Port 8080 already in use → stop whatever else is using it
```

### FRED ingest failing with 400 errors

FRED returns 400 for invalid series IDs. Verify the series ID exists by visiting `https://fred.stlouisfed.org/series/<ID>`. The series catalog in `packages/markets-pipeline/config/sources.yaml` may have a typo or a series that was renamed.

### FRED ingest failing with 429 rate limit

FRED's rate limits are generous for personal use but not infinite. If you see 429s during a large backfill, the DAG will retry with exponential backoff automatically. If it persists, chunk the backfill into smaller date ranges.

### yfinance failing intermittently

yfinance scrapes Yahoo and occasionally breaks when Yahoo changes its backend. First step: update the library (`uv pip install -U yfinance`). If that doesn't help, check the yfinance GitHub issues for known problems.

### TimescaleDB out of disk

Over time the observations hypertable will grow. TimescaleDB supports compression and retention policies:

```sql
-- Compress chunks older than 90 days (typically 90%+ compression on time-series)
SELECT add_compression_policy('observations.observations', INTERVAL '90 days');

-- Drop chunks older than 10 years (optional; only if storage is a concern)
SELECT add_retention_policy('observations.observations', INTERVAL '10 years');
```

These are commented out in the base migrations; enable explicitly when needed.

### A DAG run is stuck

```bash
# Check the task instance in Airflow UI; usually obvious from logs there.
# Last resort: force-kill the task
docker compose exec airflow-scheduler airflow tasks clear <dag_id> -s <start_date> -e <end_date>
```

### A migration failed partway

Alembic migrations should be transactional; a failed migration should roll back. If you get into an inconsistent state:

```bash
make migrate-downgrade TO=<revision-id-before-failure>
# Fix the migration
make migrate
```

Never manually edit schema out of band. The migration history is the source of truth.

## Backup and restore

### Backing up

The raw Parquet tier is the source of truth. Everything else can be rebuilt from it.

```bash
make backup-raw                         # rsync data/raw/ to $BACKUP_DEST
make backup-warehouse                   # pg_dump the warehouse
```

For offsite backup, configure `BACKUP_DEST` to point to an S3 bucket or similar. Warehouse backups are nice-to-have; raw Parquet backups are essential.

### Restoring

```bash
make restore-raw SRC=<path-to-backup>
make migrate
make seed-ontology
make rebuild-warehouse
```

This reproduces the full state from raw Parquet. Test this procedure periodically — a backup you haven't restored is a hope, not a backup.

## Updating dependencies

```bash
make update-deps                        # updates lock file with compatible versions
make update-deps-major                  # major-version bumps; review carefully
```

After updating, always run the full test suite including integration tests:

```bash
make test-all
```

## Resource limits

Default Docker Compose config assumes a laptop with 8GB+ RAM and a few GB of free disk. For longer histories or more series, you may need to:

- Increase Docker Desktop's memory allocation (12GB+ recommended).
- Use TimescaleDB compression aggressively.
- Archive old Parquet files to external storage.

## Monitoring

The pipeline writes structured logs with service/DAG/task/series context. Standard log-aggregation tools (Loki, ELK, Datadog) ingest these cleanly if you configure them. For local dev, `make logs S=<service>` is usually enough.

Key health signals to monitor in production:

- **Ingest freshness:** how old is the most recent observation per source?
  ```sql
  SELECT source_schema.source_name, MAX(as_of_date) AS most_recent
  FROM observations.observations
  GROUP BY 1;
  ```
- **DAG success rate:** Airflow exposes this via its REST API or the UI.
- **Derived-view staleness:** the gap between the latest observation and the latest derived row.
- **Classifier freshness:** most recent `regime_states.as_of_date` per classifier.

A Grafana dashboard reading from the warehouse covers all four.

## Security

**Never:**

- Commit `.env` files (already gitignored; don't work around it).
- Expose TimescaleDB or Airflow ports publicly in production. Local-only by default.
- Run Airflow with `LoadExamples=True` in production.
- Share API keys across projects. Each developer has their own FRED key.

**Always:**

- Use strong passwords for the warehouse even in local dev.
- Rotate Fernet keys if they're ever committed or exposed.
- Keep Docker images updated. `make update-images` pulls the latest base images.
