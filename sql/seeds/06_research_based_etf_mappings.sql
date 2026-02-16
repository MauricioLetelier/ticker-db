-- 06_research_based_etf_mappings.sql
-- Purpose:
-- Curated, research-based subsector mappings for ETFs that were inserted but not
-- consistently mapped as primary. This seed intentionally avoids generic fallback
-- mappings to keep classifications defensible.
--
-- Safe to re-run (idempotent).

BEGIN;

-- Candidate mappings ranked by priority (1 is preferred).
-- For some tickers we provide a fallback subsector in case the preferred
-- taxonomy node does not exist in a given database state.
WITH candidates (ticker, sector_name, subsector_name, priority, notes) AS (
  VALUES
    -- Energy
    ('AMLP', 'Energy', 'Midstream / Pipelines', 1, 'Alerian MLP infrastructure focus'),
    ('OIH',  'Energy', 'Oil & Gas Equipment & Services', 1, 'Oil services focus'),
    ('XOP',  'Energy', 'Exploration & Production', 1, 'Oil & gas E&P focus'),

    -- Financials
    ('IAK',  'Financials', 'Insurance', 1, 'Insurance industry exposure'),
    ('KBE',  'Financials', 'Banks', 1, 'Banking exposure'),
    ('KRE',  'Financials', 'Banks', 1, 'Regional banks (mapped to Banks taxonomy)'),

    -- Health Care
    ('IBB',  'Health Care', 'Biotechnology', 1, 'Biotechnology exposure'),
    ('XBI',  'Health Care', 'Biotechnology', 1, 'Biotechnology exposure'),
    ('IHF',  'Health Care', 'Health Care Services', 1, 'Healthcare providers and services'),
    ('IHF',  'Health Care', 'Managed Care', 2, 'Fallback if Services node missing'),
    ('IHI',  'Health Care', 'Health Care Equipment', 1, 'Medical devices/equipment exposure'),

    -- Industrials
    ('ITA',  'Industrials', 'Aerospace & Defense', 1, 'Aerospace and defense focus'),
    ('IYT',  'Industrials', 'Transportation', 1, 'Transportation industry exposure'),

    -- Information Technology / Thematic
    ('SMH',  'Information Technology', 'Semiconductors', 1, 'Semiconductor industry exposure'),
    ('SKYY', 'Thematic', 'Cloud Computing', 1, 'Cloud computing theme'),
    ('SKYY', 'Information Technology', 'Software', 2, 'Fallback if thematic cloud node missing'),

    -- Real Estate
    ('SRVR', 'Real Estate', 'REITs - Data Centers', 1, 'Data and infrastructure REITs'),
    ('VNQ',  'Real Estate', 'Sector ETF', 1, 'Broad real estate ETF')
),
resolved_primary AS (
  SELECT
    c.ticker,
    ss.subsector_id,
    c.notes,
    ROW_NUMBER() OVER (PARTITION BY c.ticker ORDER BY c.priority, ss.subsector_id) AS rn
  FROM candidates c
  JOIN instrument i
    ON i.ticker = c.ticker
  JOIN sector s
    ON s.sector_name = c.sector_name
  JOIN subsector ss
    ON ss.sector_id = s.sector_id
   AND ss.subsector_name = c.subsector_name
),
winner AS (
  SELECT ticker, subsector_id, notes
  FROM resolved_primary
  WHERE rn = 1
)
-- Demote current primary mapping for winners to preserve unique-primary rule.
UPDATE instrument_classification ic
SET is_primary = FALSE
FROM winner w
WHERE ic.ticker = w.ticker
  AND ic.is_primary = TRUE
  AND ic.subsector_id <> w.subsector_id;

WITH candidates (ticker, sector_name, subsector_name, priority, notes) AS (
  VALUES
    ('AMLP', 'Energy', 'Midstream / Pipelines', 1, 'Alerian MLP infrastructure focus'),
    ('OIH',  'Energy', 'Oil & Gas Equipment & Services', 1, 'Oil services focus'),
    ('XOP',  'Energy', 'Exploration & Production', 1, 'Oil & gas E&P focus'),

    ('IAK',  'Financials', 'Insurance', 1, 'Insurance industry exposure'),
    ('KBE',  'Financials', 'Banks', 1, 'Banking exposure'),
    ('KRE',  'Financials', 'Banks', 1, 'Regional banks (mapped to Banks taxonomy)'),

    ('IBB',  'Health Care', 'Biotechnology', 1, 'Biotechnology exposure'),
    ('XBI',  'Health Care', 'Biotechnology', 1, 'Biotechnology exposure'),
    ('IHF',  'Health Care', 'Health Care Services', 1, 'Healthcare providers and services'),
    ('IHF',  'Health Care', 'Managed Care', 2, 'Fallback if Services node missing'),
    ('IHI',  'Health Care', 'Health Care Equipment', 1, 'Medical devices/equipment exposure'),

    ('ITA',  'Industrials', 'Aerospace & Defense', 1, 'Aerospace and defense focus'),
    ('IYT',  'Industrials', 'Transportation', 1, 'Transportation industry exposure'),

    ('SMH',  'Information Technology', 'Semiconductors', 1, 'Semiconductor industry exposure'),
    ('SKYY', 'Thematic', 'Cloud Computing', 1, 'Cloud computing theme'),
    ('SKYY', 'Information Technology', 'Software', 2, 'Fallback if thematic cloud node missing'),

    ('SRVR', 'Real Estate', 'REITs - Data Centers', 1, 'Data and infrastructure REITs'),
    ('VNQ',  'Real Estate', 'Sector ETF', 1, 'Broad real estate ETF')
),
resolved_primary AS (
  SELECT
    c.ticker,
    ss.subsector_id,
    c.notes,
    ROW_NUMBER() OVER (PARTITION BY c.ticker ORDER BY c.priority, ss.subsector_id) AS rn
  FROM candidates c
  JOIN instrument i
    ON i.ticker = c.ticker
  JOIN sector s
    ON s.sector_name = c.sector_name
  JOIN subsector ss
    ON ss.sector_id = s.sector_id
   AND ss.subsector_name = c.subsector_name
),
winner AS (
  SELECT ticker, subsector_id, notes
  FROM resolved_primary
  WHERE rn = 1
)
INSERT INTO instrument_classification (ticker, subsector_id, is_primary, notes)
SELECT
  w.ticker,
  w.subsector_id,
  TRUE,
  w.notes
FROM winner w
ON CONFLICT (ticker, subsector_id) DO UPDATE
SET
  is_primary = EXCLUDED.is_primary,
  notes = EXCLUDED.notes;

COMMIT;
