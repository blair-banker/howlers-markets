-- Minimal ontology seed for vertical slice

-- Variables
INSERT INTO ontology.variables (name, display_name, description, tier, primary_series) VALUES
('us_10y_treasury', 'US 10-Year Treasury Yield', 'US 10-year Treasury yield', 2, 'fred:DGS10');

-- Regimes
INSERT INTO ontology.regimes (name, display_name, description, tier) VALUES
('benign_expansion', 'Benign Expansion', 'Tier 2 cluster couplings at normal strength, trending growth', 2),
('monetary_tightening', 'Monetary Tightening', 'Rates layer leading, dollar strength, risk-off', 2);

-- Regime triggers (simplified DSL)
INSERT INTO ontology.regime_triggers (regime_id, variable_id, condition, weight, description) VALUES
(1, 1, 'zscore_3m <= 1.0', 1.0, 'Benign when rates z-score is low'),
(2, 1, 'zscore_3m > 2.0', 1.0, 'Tightening when rates z-score spikes');