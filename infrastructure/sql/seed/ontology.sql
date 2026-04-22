-- Stage 4 ontology seed: 7 variables, 4 regimes, 9 triggers.
-- Idempotent (destructive): TRUNCATEs and re-seeds. Re-running wipes prior state.

BEGIN;

TRUNCATE TABLE ontology.regime_triggers RESTART IDENTITY CASCADE;
TRUNCATE TABLE ontology.regimes RESTART IDENTITY CASCADE;
TRUNCATE TABLE ontology.variables RESTART IDENTITY CASCADE;

-- Variables (IDs 1..7 after TRUNCATE + RESTART IDENTITY)
INSERT INTO ontology.variables (name, display_name, description, tier, primary_series) VALUES
  ('us_10y_treasury',  'US 10-Year Treasury Yield', 'US 10-year Treasury yield', 2, 'fred:DGS10'),
  ('us_2y_treasury',   'US 2-Year Treasury Yield',  'US 2-year Treasury yield',  2, 'fred:DGS2'),
  ('us_real_10y',      'US 10-Year Real Yield',     'US 10-year TIPS yield',     2, 'fred:DFII10'),
  ('dxy',              'US Dollar Index',           'Broad USD index (DXY)',     2, 'yfinance:DX-Y.NYB:close'),
  ('brent_oil',        'Brent Crude Oil',           'Brent crude futures (BZ=F)',2, 'yfinance:BZ=F:close'),
  ('core_cpi_yoy',     'Core CPI YoY',              'US core CPI all items less food and energy', 2, 'fred:CPILFESL'),
  ('headline_cpi_yoy', 'Headline CPI YoY',          'US CPI all urban consumers',2, 'fred:CPIAUCSL');

-- Regimes (IDs 1..4 after TRUNCATE)
INSERT INTO ontology.regimes (name, display_name, description, tier) VALUES
  ('benign_expansion',    'Benign Expansion',    'Default state; no stress regime triggered',         2),
  ('monetary_tightening', 'Monetary Tightening', 'Rates leading, dollar strengthening, real rates rising', 2),
  ('monetary_easing',     'Monetary Easing',     'Rates falling, dollar weakening, real rates dropping', 2),
  ('supply_shock',        'Supply Shock',        'Energy leading, headline inflation diverging from core', 2);

-- Regime triggers
-- monetary_tightening (regime_id=2)
INSERT INTO ontology.regime_triggers (regime_id, variable_id, condition, weight, description) VALUES
  (2, 3,
   '{"type":"compare","metric":"zscore","variable":"us_real_10y","window_days":63,"op":">","value":"1.0"}'::jsonb,
   0.4, 'Real 10y z-score 3m > 1.0'),
  (2, 4,
   '{"type":"compare","metric":"zscore","variable":"dxy","window_days":63,"op":">","value":"1.0"}'::jsonb,
   0.3, 'DXY z-score 3m > 1.0'),
  (2, 3,
   '{"type":"compare","metric":"raw_value","variable":"us_real_10y","window_days":null,"op":">","value":"0"}'::jsonb,
   0.3, 'Real 10y > 0');

-- monetary_easing (regime_id=3)
INSERT INTO ontology.regime_triggers (regime_id, variable_id, condition, weight, description) VALUES
  (3, 3,
   '{"type":"compare","metric":"zscore","variable":"us_real_10y","window_days":63,"op":"<","value":"-1.0"}'::jsonb,
   0.4, 'Real 10y z-score 3m < -1.0'),
  (3, 4,
   '{"type":"compare","metric":"zscore","variable":"dxy","window_days":63,"op":"<","value":"-1.0"}'::jsonb,
   0.3, 'DXY z-score 3m < -1.0'),
  (3, 1,
   '{"type":"compare","metric":"trend","variable":"us_10y_treasury","window_days":63,"op":"<","value":"0"}'::jsonb,
   0.3, '10y nominal trend 63d slope < 0');

-- supply_shock (regime_id=4)
INSERT INTO ontology.regime_triggers (regime_id, variable_id, condition, weight, description) VALUES
  (4, 5,
   '{"type":"compare","metric":"zscore","variable":"brent_oil","window_days":63,"op":">","value":"2.0"}'::jsonb,
   0.4, 'Brent z-score 3m > 2.0'),
  (4, 7,
   '{"type":"compare","metric":"yoy_change","variable":"headline_cpi_yoy","window_days":null,"op":">","value":"0.035"}'::jsonb,
   0.3, 'Headline CPI YoY > 3.5%'),
  (4, 7,
   '{"type":"compare_spread","left":{"metric":"zscore","variable":"headline_cpi_yoy","window_days":63},"right":{"metric":"zscore","variable":"core_cpi_yoy","window_days":63},"op":">","value":"0.5"}'::jsonb,
   0.3, 'Headline-core CPI z-score spread > 0.5');

COMMIT;
