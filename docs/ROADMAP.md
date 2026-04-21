# Roadmap

This document describes planned work beyond the initial build. Stages 1-4 of the seven-stage pipeline are the initial target; stages 5-7 are scaffolded but not implemented. Read this to understand where the project is heading and to guide design decisions in the earlier stages.

## The full seven stages

| # | Stage | Status |
|---|-------|--------|
| 1 | Ingest | **Initial build** |
| 2 | Normalize | **Initial build** |
| 3 | Derive | **Initial build** |
| 4 | Classify | **Initial build** |
| 5 | Forecast | Planned |
| 6 | Simulate | Planned |
| 7 | Reason | Planned |

## Stage 5: Forecast

**Purpose:** Produce forward-looking estimates of variables and regimes.

"Regime forecasting" is not one problem. It decomposes into several distinct prediction tasks, each of which needs its own model, its own validation, and its own interface.

### 5a. Variable path forecasts

Where will Core CPI be in 3 months? What's the expected path of 10-year yield over the next year?

**Approach:** classical time-series models. Candidates:

- **ARIMA / SARIMA** for simple univariate series with seasonal patterns.
- **Prophet** (Meta's library) for series with multiple seasonalities and structural breaks.
- **Bayesian structural time-series** (BSTS) for series where we want credible intervals and the ability to decompose trend/seasonal/regression components.
- **Vector autoregression (VAR)** for joint forecasting of the Tier 2 cluster where cross-series information matters.

**Design:** each forecast model implements a `Forecaster` protocol analogous to `Classifier`:

```python
class Forecaster(Protocol):
    name: str
    version: str

    def forecast(
        self,
        series_id: SeriesId,
        as_of_date: date,
        horizon_days: int,
    ) -> Forecast:
        """Produces mean path + confidence intervals."""
        ...
```

Outputs go to a `forecasts.variable_forecasts` hypertable keyed on `(series_id, as_of_date, horizon_days, forecaster_name, forecaster_version)`.

### 5b. Regime transition probabilities

Given the current regime state, what's the probability of transition to each other regime over the next N days?

**Approach:** hidden Markov model (HMM) fit to the derived-view variables. The latent state is the regime; the observed states are the z-scores, correlations, and spread levels. Transition matrix estimation gives per-regime transition probabilities.

**Candidate library:** `hmmlearn` for a standard Gaussian HMM; `statsmodels` for Markov-switching regressions if we want regime-conditional dynamics.

**Critical validation:** fit on pre-2020 data, validate on 2020-2026 including COVID, 2022 inflation, 2022 UK LDI, 2023 regional banks, 2026 Iran war. A model that can't label these regimes correctly in retrospect will not label the present correctly.

### 5c. Conditional forecasts

"If Brent stays above $100 for 60 days, what is the CPI path?" These are the forecasts most useful for decision support.

**Approach:** Monte Carlo simulation over the causal model. Fix the conditioning variables at their scenario values; sample the remaining variables from their conditional distributions given the ontology's coupling strengths.

**Output:** probability distributions over variable paths and regime states, conditional on user-specified scenarios.

**Honest caveat:** conditional forecasts are only as good as the causal model. The framework's coupling estimates will need to be calibrated empirically (see Stage 6).

### 5d. Tail-risk estimation

Given the current regime, what's the probability of a phase transition to a stress regime over the next N days?

**Approach:** extreme-value theory applied to the coupling-strength time series. When pairwise coupling strengths are approaching the phase-transition threshold identified by Acemoglu-Ozdaglar-Tahbaz-Salehi, tail risk is elevated.

**Output:** a scalar "phase-transition probability" per day, stored in `forecasts.tail_risk`.

## Stage 6: Simulate

**Purpose:** Shock propagation through the coupling graph. Given a perturbation to one or more variables, compute the resulting path of all others.

### Approach

Two modes:

**Deterministic propagation:** use the ontology's coupling strengths as impulse-response coefficients. Shock variable X by N standard deviations; trace the expected responses of all coupled variables over time using a structural VAR or a DebtRank-style iterative propagation.

**Stochastic propagation:** Monte Carlo over the coupling graph. Each simulation draws from the ontology's uncertainty estimates for each coupling. Output is a distribution over possible paths.

### Key use cases

- **Historical post-mortem:** "given the 2022 UK LDI shock as observed, does our model predict the realized path?" If yes, the model is calibrated; if no, find the miscalibrated couplings.
- **Forward scenarios:** "if the Fed hikes 50bp unexpectedly next week, what's the expected path of EM assets over the next quarter?"
- **Stress testing:** "in a full Tier 2 cluster phase transition, what's the expected peak drawdown on the equity portfolio?"

### Design

```python
class Simulator(Protocol):
    def simulate(
        self,
        shock: Shock,
        horizon_days: int,
        n_paths: int = 1000,
    ) -> SimulationResult:
        """Run Monte Carlo over the coupling graph."""
        ...


class Shock(BaseModel):
    variables: dict[str, Decimal]    # variable name → shock magnitude in z-scores
    duration_days: int               # how long the shock persists before mean-reverting
    as_of_date: date                 # when the shock is applied
```

Outputs go to `simulations.runs` (parameterization) and `simulations.paths` (the Monte Carlo output).

### Causal inference integration

Coupling strengths in `ontology.couplings` are initially expert judgment. As causal-discovery tooling matures, we populate them empirically:

- **Rolling Pearson/Spearman correlation** across regimes — cheap, gives descriptive edge strengths.
- **Granger causality** — does X's history predict Y beyond Y's own history? Directional but easy to misinterpret.
- **Transfer entropy** — information-theoretic, more robust to nonlinearities.
- **Structural VAR with sign restrictions** — econometric workhorse for impulse responses.
- **Bayesian network / DAG discovery** (PC algorithm, NOTEARS) — infers full causal structure under strong assumptions.

Each method writes to `ontology.couplings` with its method name recorded in `estimation_method`. Multiple estimates per edge are expected and useful.

**Honest caveat:** regime-conditional causality in markets is partially unidentifiable from observational data alone. Simulator output is decision support, not prediction. Communicate uncertainty visibly.

## Stage 7: Reason

**Purpose:** Produce grounded, auditable narrative explanations of what the pipeline has classified, forecast, and simulated.

### The problem

The lower stages produce structured outputs: regime labels, forecasts, simulation distributions. These are useful for a professional user who can read them, but the framework we've built is meant to be a reasoning aid, and a regime label without context fails at that job.

At the same time, letting an LLM free-form explain market state is the exact kind of hallucination risk the framework was designed to resist. The reasoning layer must be grounded.

### Approach

The reasoning layer is a consumer of structured state, not a generator of claims. It:

1. Reads the current `regime_states`, `forecasts.*`, and (when available) `simulations.*` for the requested as-of date.
2. Reads the relevant ontology context (which variables triggered the classification, which couplings are active).
3. Produces a narrative explanation *grounded in those structured inputs*, with every claim tagged to the data that supports it.
4. Never generates claims not supported by the structured state.

**Design pattern:** template-driven narrative generation with LLM-assisted prose refinement, not open-ended LLM narration.

```python
class Explainer(Protocol):
    def explain(
        self,
        as_of_date: date,
        audience: Literal["practitioner", "educated_layman"] = "practitioner",
    ) -> Explanation:
        """Produce grounded narrative."""
        ...


class Explanation(BaseModel):
    summary: str
    regime: str
    key_drivers: list[Citation]      # each key driver is tagged to a data point
    risks: list[Citation]
    confidence: Decimal
    audit_trail: list[str]           # the queries that produced this explanation
```

### Validation

For every historical episode with a known retrospective characterization (2022 UK LDI, 2023 SVB, 2026 Iran war), the explainer should produce an explanation that:

1. Names the correct regime.
2. Identifies the correct trigger variables.
3. Doesn't hallucinate variables or couplings not in the data.
4. Expresses appropriate uncertainty.

A held-out test set of historical episodes is the validation benchmark.

### Integration with the framework

The reasoning layer should use the framework's own vocabulary — Tier 2 cluster, phase transition, sovereign-bank doom loop, term premium — because that vocabulary is the framework's ontology and is checkable against what's in the database.

## Cross-cutting future work

### Additional data sources

Planned sources beyond the initial FRED + yfinance + Treasury + BLS + EIA:

- **IMF / World Bank** — sovereign debt, current account, reserve composition.
- **BIS** — cross-border banking claims, OTC derivatives positions.
- **US Treasury TIC data** — foreign holdings of US securities, flow data.
- **SEC N-PORT filings** — mutual fund and ETF holdings (quarterly, lagged).
- **CME Group** — futures positioning (Commitments of Traders).
- **Non-financial alternative data** — shipping rates (Baltic Dry), electricity demand, satellite imagery proxies. Speculative; evaluate only if clearly additive.

### Cross-asset expansion

Initial series set is macro-heavy. Planned additions:

- **Equity sector and factor returns** — for Tier 3 analysis.
- **Single-name equity for the critical infrastructure list** — TSMC, ASML, major energy and defense.
- **Commodities beyond oil** — industrial metals (copper, iron), agriculture (wheat, corn), precious (gold, silver).
- **Crypto** — BTC and ETH, treated as a satellite asset class, not foundational.

### Dashboard

Streamlit MVP in the initial build; React + FastAPI in the long run. Features:

- **Current state view** — matrix with live values and z-scores per cell, coupling graph with current strengths.
- **Regime timeline** — historical regime states over a selectable date range.
- **Variable explorer** — time-series charts with vintage toggles (see data as it was known on date X).
- **Coupling explorer** — graph view with filtering by regime, coupling type, tier.
- **Forecast / simulation UI** — once Stages 5-6 are built.

### Export and APIs

A lightweight REST API (FastAPI) exposing the warehouse to external consumers. Useful for integrating with spreadsheets, notebooks, and third-party dashboards. Not in the initial build.

## Sequencing

Suggested order for the post-initial work:

1. Stage 4 completion and validation against historical episodes (COVID, 2022 inflation, 2026 Iran war).
2. Stage 5a (variable path forecasts) — simplest forecast work, big value-add.
3. Dashboard v1 — now that there's something worth looking at.
4. Stage 5b (HMM regime transitions) — natural follow-on.
5. Causal-discovery tooling — populates `ontology.couplings` empirically.
6. Stage 6 (simulator) — once couplings are empirically grounded.
7. Stage 5c/5d (conditional forecasts, tail risk) — builds on simulator.
8. Stage 7 (reasoning layer) — last, because it's a consumer of everything.

Each stage should be shippable independently. The product should be useful with just Stages 1-4 complete; it should be more useful with each subsequent stage added; but no stage should be a prerequisite for its predecessors to work.

## Known hard problems

Flagging these explicitly so they don't come as surprises:

- **Data revisions and vintage complexity.** Point-in-time discipline is correct, but some sources (BLS especially) don't expose vintage information cleanly. Reconstructing what was known on a given historical date is partial.
- **Regime labeling is partly subjective.** Even professional macro shops disagree on whether a specific month was "monetary tightening" or "fiscal dominance." Our classifier outputs probabilities, not certainties, and the probability distribution itself is imperfect.
- **Coupling strengths are non-stationary.** The graph structure changes across regimes; calibrating couplings on pre-crisis data and applying to crisis data gives bad answers.
- **Causal identification.** Observational data from markets rarely supports strong causal claims. Best-case we identify directional influence and rank edges by evidence strength.
- **LLM grounding.** The reasoning layer's fundamental challenge — producing explanations grounded in computed state without going off-script — is an open problem with no universal solution yet.

These are not reasons to avoid the work. They are reasons to ship the work with honest uncertainty communication.
