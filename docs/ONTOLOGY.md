# The Markets Framework (Ontology)

This document describes the conceptual framework that the pipeline operationalizes: the matrix, the tier architecture, the coupling graph, and the regime types. It is both a reference for developers and a source of ground truth for classifier and forecaster logic. Read this before touching anything that interprets market state.

The ontology also exists as *data* in the `ontology` schema of the warehouse. When the code needs to know about a variable, a coupling, or a regime, it queries the schema rather than hardcoding. This document describes what's in those tables and why.

## The two-object architecture

The framework has two complementary objects:

1. **The matrix** — a static ontology. Classifies every variable by *kind* (11 columns) and *causal depth* (10 rows). Answers the question "what is this thing and how foundational is it?"
2. **The coupling graph** — a dynamic overlay. Captures which variables *move with which*, at what strength, under which regimes. Answers the question "what does this move with?"

Neither alone is sufficient. The matrix gives you the ontology; the graph gives you the dynamics. They connect through shared variable identity — every node in the graph is a row in the matrix.

## The matrix

Rows are causal depth: 1 is most foundational (preconditions), 10 is most reflexive (fast-moving dynamics). Columns are taxonomic kind.

### Depth rows (ontology.depth_rows)

| Level | Name | Description |
|-------|------|-------------|
| 1 | `preconditions` | The slow-moving foundations that everything else presupposes |
| 2 | `institutional_foundations` | State capacity, central bank credibility, regulatory frameworks |
| 3 | `monetary_credit_foundations` | Money supply, debt issuance, collateral, banking structure |
| 4 | `price_of_money` | Policy rates, real rates, yield curves, credit spreads |
| 5 | `real_economy_substrate` | Labor, energy, demographics, productivity, critical inputs |
| 6 | `cross_border_transmission` | FX, capital flows, sanctions, trade, chokepoints |
| 7 | `financial_transmission` | Repo, dealers, derivatives, MMFs, NBFI propagation |
| 8 | `cash_flow_generation` | Earnings, consumption, defaults, market access |
| 9 | `regime_shock` | Wars, regime shifts, currency crises, plumbing failures |
| 10 | `reflexive_behavior` | Momentum, narratives, deleveraging spirals, phase transitions |

Rows 1-6 form a roughly ontological progression: each presupposes the rows beneath it. Rows 7-10 are less hierarchical — they describe dynamic modes that can strike at any depth.

### Columns (ontology.columns)

| Name | Display | Description |
|------|---------|-------------|
| `legal_political` | Legal / Political | State capacity, rule of law, specific decision-makers (FOMC, OPEC+) |
| `monetary_credit` | Monetary / Credit | The price of money and institutions that manage it |
| `fiscal_sovereign` | Fiscal / Sovereign | Government debt, deficits, sovereign creditworthiness |
| `real_economy` | Real Economy / Resources | Energy, labor, demographics, productivity, critical inputs |
| `external_fx_trade` | External / FX / Trade | Cross-border transmission, reserve status, chokepoints |
| `financial_plumbing` | Financial Plumbing | Settlement rails, repo, clearing, NBFI |
| `corporate_household` | Corporate / Household | Earnings, consumption, leverage, default cycles |
| `behavioral` | Behavioral | Expectations, risk tolerance, principal-agent dynamics, narratives |
| `information_epistemic` | Information / Epistemic | Disclosure, rating agencies, financial media, AI-generated analysis |
| `market_structure` | Market Structure | Microstructure, flows, time-horizon diversity, passive/HFT dynamics |
| `physical_environmental` | Physical / Environmental | Climate, water, grid, pandemics, physical infrastructure |

The last three columns were added to the framework late and capture dimensions that earlier frameworks had treated as externalities. Treat them as first-class.

## The tier architecture

Orthogonal to the matrix; a cleaner view of causal dynamics.

### Tier 1: Preconditions

Slow-moving, foundational, decade-to-century timescales. Examples: rule of law, reserve-currency status, demographic structure, geopolitical order. These don't move day-to-day but their erosion violently repices everything above them.

### Tier 2: Core coupled cluster

Fast-moving, tightly interconnected. Examples: US risk-free rate, USD, oil, inflation, sovereign credit. These form a subgraph where causality runs in multiple directions simultaneously. Which node leads depends on the shock type, not the variables themselves.

**The Tier 2 cluster is the single most important thing to track.** It reprices in seconds and it is where most near-term market action originates.

### Tier 3: Derived prices

Downstream of Tier 2, mostly driven rather than driving. Examples: equity multiples, credit spreads, housing, EM assets, corporate earnings. They feed back through wealth effects and financial conditions but are secondary drivers.

## The coupling graph

A directed graph where nodes are variables and edges are empirically observed couplings with regime-conditional strengths.

### Coupling types (ontology.couplings.coupling_type)

- **`reflexive`** — tight bidirectional coupling within a cluster. Tier 2 cluster members have this.
- **`doom_loop`** — feedback loop where stress in A weakens B which in turn weakens A. Sovereign-bank is the canonical example.
- **`collateral`** — coupling through a shared collateral chain (Treasury ↔ repo ↔ dealers ↔ basis trades).
- **`funding`** — coupling through shared funding leg (the carry-trade cluster).
- **`expectations`** — coupling through shared expectations mechanism (policy rates ↔ inflation expectations ↔ term premium).
- **`common_factor`** — coupling through shared exposure to an exogenous factor (global risk appetite, dollar liquidity).
- **`regulatory`** — coupling via shared regulatory constraints (Basel-linked balance sheet limits).

### Key named clusters

These are not arbitrary — each has a body of empirical literature and recent historical instantiations.

**Tier 2 macro cluster**
- Members: US 10y, USD, Brent, Core CPI, US sovereign credit
- Type: `reflexive`
- Normal strength: ~0.4-0.6 pairwise
- Stress strength: ~0.7-0.9 pairwise
- Historical activation: Q1 2022 (monetary tightening), Q1 2026 (Iran war)

**Sovereign-bank doom loop**
- Members: domestic banks ↔ domestic sovereign debt
- Type: `doom_loop`
- Activation: Eurozone 2011-12, UK gilts Q3 2022
- Reference: Acemoglu-Ozdaglar-Tahbaz-Salehi 2015 on phase-transition dynamics

**Treasury-repo-dealer cluster**
- Members: Treasury prices, repo rates, primary-dealer balance sheets, basis trades
- Type: `collateral`
- Activation: September 2019 repo spike, March 2020 Treasury basis unwind, April 2026 auction stress
- Reference: Brunnermeier-Pedersen 2009 on liquidity spirals

**Yen carry cluster**
- Members: JPY, EM FX basket, risk assets, BOJ policy expectations
- Type: `funding`
- Activation: August 2024 yen carry unwind
- Character: dormant in low-vol regimes, explosive in regime shifts

**Credit-equity-vol cluster**
- Members: S&P 500, IG OAS, HY OAS, VIX
- Type: `common_factor` (global risk appetite)
- Character: decouples in liquidity stress, recouples in fundamentals-driven stress

## Regime types (ontology.regimes)

Named states of the market system. A classifier places the current state into one (or a probability distribution over several).

### Named regimes in the initial definition set

- **`benign_expansion`** — Tier 2 cluster couplings at normal strength, trending growth, anchored inflation expectations. Default state.

- **`monetary_tightening`** — rates layer (4) leading. Dollar strength, risk-off in duration-sensitive assets, real rates rising. Q1 2022 canonical.

- **`monetary_easing`** — mirror of tightening. Rates falling, risk-on, dollar weakness, TIPS breakevens firming.

- **`supply_shock`** — real-economy substrate (5) leading, specifically energy or food. Headline inflation diverging from core, term premium rising, central bank on hold. Q1 2026 instantiation.

- **`fiscal_dominance`** — sovereign credit leading. Term premium rising independently of short rates, weak Treasury auctions, convenience yield compressing. Historical rarity in reserve-currency nations; emerging risk in 2026.

- **`funding_stress`** — financial plumbing (7) leading. Repo stress, basis-trade unwind, dealer retreat. Sept 2019, March 2020 canonical.

- **`emerging_market_crisis`** — cross-border (6) leading. USD strength, EM FX collapse, capital flight. 1997, 2013 taper tantrum, 2022 partial.

- **`institutional_shock`** — layer 1-2 leading. War, constitutional crisis, sanctions. Propagates *upward* through all columns.

- **`phase_transition`** — a transient regime capturing the moment a coupling network crosses from absorptive to amplifying. Short-lived; resolves into one of the stress regimes.

Regime definitions (trigger conditions) are in `ontology.regime_triggers`. They can be updated via migration without code changes.

## Causal dependencies — what the framework says

Critical caveats before quantitative use.

### What the framework asserts

1. **Tier 2 cluster coupling increases under stress.** This is well-supported by the Acemoglu-Ozdaglar-Tahbaz-Salehi phase-transition result: dense networks absorb small shocks and amplify large ones.

2. **Indirect effects dominate direct effects in stress regimes.** Second and third-round propagation often exceed first-round impact by 3-6x (Bardoscia-Battiston work).

3. **Causality direction within the cluster depends on shock type.** There is no "true" driver; the driver is whichever node was hit first.

4. **Non-bank financial intermediaries (NBFIs) are the current system's weakest plumbing.** Post-2008 regulation hardened banks and pushed risk to non-bank intermediaries with weaker visibility. Recent IMF work supports this.

5. **Information and narrative are structural, not decorative.** The same fundamental shock produces different market outcomes depending on how it is narrated.

### What the framework does NOT assert

1. **Specific point forecasts.** The framework is about structure and regime, not about where variables will be next week.

2. **That any classifier output is ground truth.** Regime labels are probabilistic judgments, not observations.

3. **That the ontology is complete.** The matrix has evolved (three columns added in the late-2025 revision) and will evolve further.

4. **That couplings are stationary.** Coupling strengths change across regimes by design; the graph is not a single static object.

## How the code uses the ontology

**Classifiers** query `ontology.regimes` and `ontology.regime_triggers` to know which regimes exist and what their trigger conditions are. Adding a new regime is an insert, not a code change.

**Transforms** query `ontology.variables` to know which series belong to which matrix cell and which tier. Computing the "Tier 2 cluster correlation matrix" is a join against `ontology.variables WHERE tier = 2`.

**Dashboards** query the ontology to render the matrix, the coupling graph, and the regime timeline in a framework-aware way.

**Future forecasters and simulators** will query `ontology.couplings` to know which variables move together and with what strength. This is why the coupling table exists from day one even though its contents are seeded with expert judgment; as causal-discovery tooling comes online (Granger, transfer entropy), it will populate the same table with estimation metadata.

## Evolving the ontology

Changes go through data migrations in `infrastructure/sql/seed/ontology_migrations/`. Rules:

1. **Never delete a variable or regime.** Mark them deprecated with a column in the table. Classifier outputs referencing them must remain queryable.
2. **Document the reasoning.** Each migration includes a commit message explaining why the change is warranted.
3. **Test before deploy.** A migration that changes regime triggers must be backtested against historical episodes to verify it doesn't produce regressions.

## References

Source material this framework draws on (not a complete bibliography — see individual paper citations in code comments where models implement specific methods):

- Acemoglu, Ozdaglar, Tahbaz-Salehi (2015). "Systemic Risk and Stability in Financial Networks." *AER* 105(2). Phase-transition result for network fragility.
- Battiston, Puliga, Kaushik, Tasca, Caldarelli (2012). "DebtRank: Too Central to Fail?" *Scientific Reports* 2:541. Centrality-based systemic importance.
- Haldane, May (2011). "Systemic risk in banking ecosystems." *Nature* 469. "Robust-yet-fragile" framing.
- Brunnermeier, Pedersen (2009). "Market Liquidity and Funding Liquidity." *RFS* 22(6). Liquidity spirals.
- Shin, H. S. BIS work on the "second phase of global liquidity" and cross-border dollar transmission.
- IMF/BIS/FSB (2009). G-SIB identification framework: size + interconnectedness + substitutability.
- IMF (2026). "Risk Propagation in the European Banking System: Amplification Effect from NBFIs and Market Risks." NBFI amplification evidence.
- Taleb (2007). *The Black Swan*. Limits of quantification; convexity over point forecasts.
