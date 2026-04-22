# Stage 4: Rule-Based Classifier — Design

**Date:** 2026-04-22
**Status:** Approved for implementation planning
**Scope:** Stage 4 end-to-end, with the minimum Stage 3 slice required to support it, plus a minimum yfinance source to supply two variables the classifier needs.

---

## 1. Goals

Ship a working Stage 4 classifier that:

- Distinguishes four named regimes: `benign_expansion`, `monetary_tightening`, `monetary_easing`, `supply_shock`.
- Reads derived metrics point-in-time (strict `as_of_date` discipline).
- Produces one `regime_states` row per logical date, with a weighted-score winner, confidence, structured rationale, and auditable classifier + ontology version.
- Conforms to the `Classifier` protocol defined in `markets-core`; passes the shared contract test suite.
- Supports both normal daily-snapshot runs (via the `classify` DAG) and explicit history replay (via `make backfill-classify`).

Non-goals: HMM classifier, forecasting, narrative prose generation (Stage 7), dashboard changes, the remaining five regimes from `ONTOLOGY.md`, empirical calibration of trigger thresholds.

## 2. Project-wide rule added

One cross-cutting change, added to `CLAUDE.md` as a non-negotiable alongside the existing append-only and point-in-time rules:

> **No sub-daily data.** All series are stored at daily granularity or coarser. Market variables that natively trade intraday are stored as daily OHLC (open/high/low/close). Lower-frequency series (e.g., monthly CPI) are forward-filled to daily only at the classifier read boundary — the underlying storage remains at native frequency with point-in-time vintages preserved.

This is a project-wide invariant, not specific to Stage 4. Future source implementations must honor it.

## 3. Architecture

Three layers, mirroring the existing repo structure.

### 3.1 In `markets-core` (abstractions)

- **`domain/regime.py`** — new. Pydantic models:
  - `Regime` — name, display name, description, tier.
  - `RegimeTrigger` — regime id, variable id, condition (JSON AST, see §4), weight, description. Validated via Pydantic on load.
  - `RegimeClassification` — classifier output for a single as-of date: regime name, confidence, trigger variables, rationale text, rationale detail (structured), classifier name, classifier version, ontology version.
- **`interfaces/classifier.py`** — new. The `Classifier` protocol:
  ```python
  @runtime_checkable
  class Classifier(Protocol):
      name: str
      version: str
      def classify(self, as_of_date: date) -> RegimeClassification: ...
  ```
- **`errors.py`** — extended. Add `InsufficientDataError` as a subclass of `MarketsCoreError`.
- **`tests/contract/test_classifier_contract.py`** — new. Shared contract suite (see §7).

### 3.2 In `markets-pipeline` (implementations)

- **`transforms/zscore.py`**, **`transforms/yoy_change.py`**, **`transforms/trend.py`** — three `Transform` implementations. Each reads `observations.observations` point-in-time, forward-fills monthly series to daily at read time, and writes to its own `derived.*` table keyed on `(series_id, observation_date, as_of_date, window_days)`.
- **`classifiers/rule_based.py`** — the `RuleBasedClassifier`. Reads ontology + derived views point-in-time, evaluates trigger conditions, computes per-regime weighted scores, picks winner, returns `RegimeClassification`.
- **`classifiers/conditions.py`** — pure-function condition evaluator. `evaluate(condition: ConditionNode, features: dict[str, Decimal | None]) -> bool`. Walks the JSON AST; no string parsing, no `eval`.
- **`sources/yfinance.py`** — new, minimally scoped. Implements the `Source` protocol; pulls daily OHLC for two series in scope (`DX-Y.NYB`, `BZ=F`). Passes the shared `Source` contract suite. Not intended as a full-featured yfinance integration — additional series can be added later.
- **`dags/classify.py`** — Airflow DAG. Triggered by the `derived_views` Airflow dataset. One run per logical date writes one `regime_states` row (upsert).
- **`dags/ingest_yfinance.py`** — minimal DAG for the two yfinance series.
- **`scripts/backfill_classify.py`** — explicit history-replay command invoked by `make backfill-classify START=... END=...`.

### 3.3 In `infrastructure/sql`

- **One additive Alembic migration** containing:
  - Create `derived.zscores`, `derived.yoy_changes`, `derived.trends` hypertables (§5).
  - Alter `ontology.regime_triggers.condition` from `text` to `jsonb`.
  - Alter `regimes.regime_states`: add `rationale_detail jsonb NOT NULL DEFAULT '{}'::jsonb`, add `ontology_version text NOT NULL DEFAULT ''`.
- **`seed/ontology.sql`** — seeds the 7 variables, 4 regimes, and trigger rows in §6.

### 3.4 Data flow on one classify run at as-of D

```
ontology.regimes + ontology.regime_triggers (latest migration)
                              │
                              ▼
                   RuleBasedClassifier(as_of=D)
                              │
                              │  (for each required variable × metric)
                              ▼
          derived.{zscores, yoy_changes, trends}
             point-in-time view WHERE as_of_date ≤ D
                              │
                              ▼
              evaluate each regime's triggers
                              │
                              ▼
                weighted score per regime
                              │
                              ▼
       winner + confidence + rationale + rationale_detail
                              │
                              ▼
                regimes.regime_states (one row, upsert)
```

What the classifier does NOT do: fetch raw observations, compute its own metrics, write to tables other than `regime_states`, know anything about data providers.

## 4. Trigger condition AST

Trigger conditions are stored as JSON in `ontology.regime_triggers.condition`. Validated via a Pydantic discriminated union. Node types:

**Leaf — scalar comparison of a single metric:**
```json
{
  "type": "compare",
  "metric": "zscore",          // zscore | yoy_change | trend | raw_value
  "variable": "brent_oil",     // ontology.variables.name
  "window_days": 63,           // required for zscore and trend; null otherwise
  "op": ">",                   // > | >= | < | <= | ==
  "value": 2.0
}
```

**Leaf — scalar comparison of the spread between two metrics:**
```json
{
  "type": "compare_spread",
  "left":  {"metric": "zscore", "variable": "headline_cpi_yoy", "window_days": 63},
  "right": {"metric": "zscore", "variable": "core_cpi_yoy",     "window_days": 63},
  "op": ">",
  "value": 0.5
}
```

**Compound — boolean combinations:**
```json
{"type": "all_of", "conditions": [ ... ]}
{"type": "any_of", "conditions": [ ... ]}
```

The evaluator is a small recursive function over validated Pydantic models. A missing metric value is never silently treated as False. Two cases:

1. **Variable not observed at all by as-of D** (point-in-time read returns no row) → classifier raises `InsufficientDataError` before evaluation begins. DAG task fails loudly.
2. **Variable observed but metric undefined** (e.g., 3-month z-score requested for a variable with < 63 days of history at as-of D; transform wrote `NULL`) → classifier raises `InsufficientDataError` with an explicit "insufficient history" message. This prevents the very early part of a backfill from producing meaningless low-confidence classifications.

## 5. Schemas

### 5.1 Derived tables

Three hypertables, same shape except for the value column:

```sql
CREATE TABLE derived.zscores (
    series_id text NOT NULL,
    observation_date date NOT NULL,
    as_of_date date NOT NULL,
    window_days integer NOT NULL,   -- 63 (~3m) or 252 (~12m)
    zscore numeric(10,4),
    computed_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (series_id, observation_date, as_of_date, window_days)
);
SELECT create_hypertable('derived.zscores', 'observation_date');

CREATE TABLE derived.yoy_changes (
    series_id text NOT NULL,
    observation_date date NOT NULL,
    as_of_date date NOT NULL,
    yoy_pct numeric(10,6),          -- fractional, e.g. 0.035 = 3.5%
    computed_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (series_id, observation_date, as_of_date)
);
SELECT create_hypertable('derived.yoy_changes', 'observation_date');

CREATE TABLE derived.trends (
    series_id text NOT NULL,
    observation_date date NOT NULL,
    as_of_date date NOT NULL,
    window_days integer NOT NULL,
    slope numeric(10,6),            -- OLS slope of value vs day index over window
    computed_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (series_id, observation_date, as_of_date, window_days)
);
SELECT create_hypertable('derived.trends', 'observation_date');
```

### 5.2 `regime_states` additions

```sql
ALTER TABLE regimes.regime_states
  ADD COLUMN rationale_detail jsonb NOT NULL DEFAULT '{}'::jsonb,
  ADD COLUMN ontology_version text NOT NULL DEFAULT '';
```

`rationale_detail` example:
```json
{
  "winner": "monetary_tightening",
  "score": 0.82,
  "regime_scores": {
    "monetary_tightening": 0.82,
    "monetary_easing": 0.10,
    "supply_shock": 0.25,
    "benign_expansion": 0.18
  },
  "triggers": [
    {"regime": "monetary_tightening", "name": "rate_rising",
     "satisfied": true, "weight": 0.4,
     "metric": "zscore", "variable": "us_real_10y", "window_days": 63,
     "value": 2.1, "op": ">", "threshold": 1.0},
    ...
  ],
  "tiebreak_applied": false
}
```

`ontology_version` is populated at classifier construction time from the latest `alembic_version` revision ID.

### 5.3 `ontology.regime_triggers` change

`condition` column changes from `text` to `jsonb`. Existing rows (if any — the table is currently unseeded) are not migrated; the seed script populates fresh JSON values.

## 6. Seed content

### 6.1 Variables (7)

| Name | Primary series | Native freq | Column (matrix) | Depth row |
|---|---|---|---|---|
| `us_10y_treasury` | `fred:DGS10` | daily | `monetary_credit` | `price_of_money` |
| `us_2y_treasury` | `fred:DGS2` | daily | `monetary_credit` | `price_of_money` |
| `us_real_10y` | `fred:DFII10` | daily | `monetary_credit` | `price_of_money` |
| `dxy` | `yfinance:DX-Y.NYB` | daily | `external_fx_trade` | `cross_border_transmission` |
| `brent_oil` | `yfinance:BZ=F` | daily | `real_economy` | `real_economy_substrate` |
| `core_cpi_yoy` | `fred:CPILFESL` | monthly | `real_economy` | `real_economy_substrate` |
| `headline_cpi_yoy` | `fred:CPIAUCSL` | monthly | `real_economy` | `real_economy_substrate` |

Monthly series are forward-filled to daily at the derived-view read boundary, not in storage.

### 6.2 Regimes and triggers

Scoring rule: `score(regime) = Σ(weight_i × satisfied_i) / Σ(weight_i)` for each regime. Winner is the highest-scoring regime; if no stress regime scores ≥ 0.5, `benign_expansion` wins with confidence `1 - max(stress_scores)`.

**`monetary_tightening`** — rates leading, dollar strengthening, real rates positive.
- `zscore_3m(us_real_10y) > 1.0` — weight 0.4
- `zscore_3m(dxy) > 1.0` — weight 0.3
- `raw_value(us_real_10y) > 0` — weight 0.3

**`monetary_easing`** — mirror of tightening.
- `zscore_3m(us_real_10y) < -1.0` — weight 0.4
- `zscore_3m(dxy) < -1.0` — weight 0.3
- `trend_63d(us_10y_treasury) < 0` — weight 0.3

**`supply_shock`** — energy leading, headline inflation diverging from core.
- `zscore_3m(brent_oil) > 2.0` — weight 0.4
- `yoy_change(headline_cpi_yoy) > 0.035` — weight 0.3
- `spread(zscore_3m(headline_cpi_yoy), zscore_3m(core_cpi_yoy)) > 0.5` — weight 0.3

**`benign_expansion`** — default; no explicit triggers. Wins when `max(score_over_stress_regimes) < 0.5`.

Thresholds and weights are expert-judgment starting points, not calibrated. Empirical tuning is explicitly out of scope for this build.

### 6.3 Tiebreak

When two or more regimes score equal: choose the regime with the highest total trigger weight (the sum of weights of its satisfied triggers); if still tied, alphabetical by regime name. `rationale_detail.tiebreak_applied` is set to `true` when the tiebreak path is taken.

## 7. Testing

Four layers, following `docs/TESTING.md`.

### 7.1 Unit tests (fast, no I/O)

- `RegimeTrigger` condition AST validation — every node type, malformed JSON raises `ValidationError`.
- Condition evaluator — every AST node type including `compare_spread`, compound `all_of` / `any_of`, edge cases (equality, negation via compound).
- Weighted-score math — normalization, all-satisfied, none-satisfied, partial.
- Tiebreak logic — equal-score paths, deterministic ordering.
- Benign-expansion default — triggered only when no stress regime scores ≥ 0.5; confidence formula.

### 7.2 Contract tests

- **`Classifier` protocol** — shared suite at `packages/markets-core/tests/contract/test_classifier_contract.py`:
  - Output shape matches `RegimeClassification`.
  - `confidence` ∈ [0, 1].
  - Idempotency: two calls with the same as-of date and unchanged ontology return identical output (including `ontology_version`).
  - `ontology_version` is non-empty and matches the latest Alembic revision.
- **`Transform` protocol** — the three new transforms run the existing `Transform` contract suite. One point-in-time invariant is explicit: a transform result for `as_of_date = D` uses only observations with `as_of_date ≤ D` (verified by constructing a synthetic vintage history).
- **`Source` protocol** — the new `YfinanceSource` runs the existing `Source` contract suite.

Contract test runs against `RuleBasedClassifier` with a fixture Postgres (test-containers).

### 7.3 Integration tests

- End-to-end: seed → ingest (FRED + yfinance for a narrow window) → transform → classify → assert a `regime_states` row exists and matches the expected shape.
- Migration test: applying the new migration to an empty DB produces the expected schema.

### 7.4 Historical regression (sanity, not calibration)

Three hand-picked episodes where the correct label is obvious from the history. Marked `@pytest.mark.integration` + `@pytest.mark.slow`; run manually and in CI on merges to main.

| As-of date | Expected regime | Reason |
|---|---|---|
| 2020-04-15 | `monetary_easing` | Fed emergency cuts, dollar easing |
| 2022-10-15 | `monetary_tightening` | Aggressive Fed hiking cycle |
| 2022-03-15 | `supply_shock` | Oil spike post-Ukraine invasion |

Assertion: classifier returns the expected regime with confidence > 0.5. If an episode fails, either the thresholds get tuned in the seed or the assertion is relaxed with an explicit `# TODO: calibrate` note. This is a smoke test for design correctness, not a validation suite.

## 8. Error handling

All errors raise exceptions from the `MarketsCoreError` hierarchy. No silent degradation.

| Failure | Exception | Behavior |
|---|---|---|
| Point-in-time read finds no value for a required variable at as-of D | `InsufficientDataError` | DAG task fails loudly; no `regime_states` row written |
| Trigger condition JSON fails Pydantic validation | `ConfigurationError` | Raised at classifier construction; no run proceeds |
| Unknown metric name / variable name in a trigger | `ConfigurationError` | Raised at classifier construction |
| Tie between two or more regime scores | None — deterministic tiebreak, recorded in `rationale_detail.tiebreak_applied` |

Provider-side failures for the yfinance source follow the existing `Source` convention: wrap in `DataSourceError` / `SeriesNotFoundError` at the source boundary.

## 9. Operations

Three Make targets touched:

- `make migrate` (existing) — applies the new Alembic migration.
- `make seed-ontology` (existing; newly populated) — seeds the 7 variables, 4 regimes, triggers.
- `make backfill-classify START=YYYY-MM-DD END=YYYY-MM-DD` — new. Explicit history replay; writes one `regime_states` row per business day in range. Idempotent (upsert).

DAG orchestration per `ARCHITECTURE.md`: `classify` DAG is triggered by the `derived_views` Airflow dataset. One run per logical date. Task retries follow existing Airflow policy.

## 10. Component inventory

New files:

```
packages/markets-core/src/markets_core/
  domain/regime.py
  interfaces/classifier.py

packages/markets-core/tests/contract/
  test_classifier_contract.py

packages/markets-pipeline/src/markets_pipeline/
  classifiers/__init__.py
  classifiers/rule_based.py
  classifiers/conditions.py
  sources/yfinance.py
  transforms/__init__.py
  transforms/zscore.py
  transforms/yoy_change.py
  transforms/trend.py
  dags/ingest_yfinance.py

packages/markets-pipeline/tests/
  classifiers/test_rule_based_contract.py
  classifiers/test_conditions.py
  transforms/test_zscore_contract.py
  transforms/test_yoy_change_contract.py
  transforms/test_trend_contract.py
  sources/test_yfinance_contract.py

scripts/backfill_classify.py

infrastructure/sql/migrations/
  <next>_stage4_classifier.py     # the single additive migration

infrastructure/sql/seed/
  ontology.sql                    # overwritten/extended
```

Modified files:

```
packages/markets-core/src/markets_core/errors.py       # add InsufficientDataError
packages/markets-pipeline/src/markets_pipeline/dags/classify.py   # fill in (currently stub)
packages/markets-pipeline/src/markets_pipeline/dags/derive.py     # wire new transforms
CLAUDE.md                                              # add "no sub-daily data" rule
Makefile                                               # add backfill-classify target
docs/DATA-MODEL.md                                     # note jsonb change, new columns
docs/ONTOLOGY.md                                       # note that the 4 regimes are seeded; others deferred
```

## 11. Out of scope (explicit)

- HMM or ML-based classifier (planned, separate work per `ROADMAP.md`).
- The remaining five regimes from `ONTOLOGY.md` (`funding_stress`, `fiscal_dominance`, `emerging_market_crisis`, `institutional_shock`, `phase_transition`).
- Empirical calibration of trigger thresholds and weights.
- Narrative prose generation (Stage 7 responsibility).
- Dashboard changes.
- Additional data sources beyond the minimum yfinance slice (DXY + Brent only).
- Additional derived views beyond z-score, YoY change, trend.

These are all additive follow-ups. None block this design; each has a clean extension point (new regime = seed insert + optionally new metric transform; new classifier = new implementation of `Classifier` protocol).
