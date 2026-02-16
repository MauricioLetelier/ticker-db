-- 07_research_proxy_subsector_coverage.sql
-- Purpose:
-- Fill remaining empty subsectors with researched proxy ETF mappings while
-- keeping primary mappings untouched.
--
-- Safe to re-run (idempotent).

BEGIN;

WITH proxy_map (sector_name, subsector_name, ticker, notes) AS (
  VALUES
    ('Commodities', 'Sector ETF', 'GLD', 'Proxy coverage: broad commodities anchor via gold'),

    ('Communication Services', 'Entertainment', 'VOX', 'Proxy coverage: comm services basket including entertainment'),
    ('Communication Services', 'Interactive Media & Services', 'VOX', 'Proxy coverage: comm services basket including interactive media'),
    ('Communication Services', 'Internet & Direct Marketing Retail', 'FDN', 'Proxy coverage: internet-focused ETF'),
    ('Communication Services', 'Media', 'VOX', 'Proxy coverage: comm services/media exposure'),

    ('Consumer Discretionary', 'Consumer Durables & Apparel', 'FXD', 'Proxy coverage: discretionary blend incl. durables/apparel'),
    ('Consumer Discretionary', 'Consumer Services', 'PEJ', 'Proxy coverage: consumer services via leisure/services'),

    ('Consumer Staples', 'Beverages', 'VDC', 'Proxy coverage: staples basket includes beverages'),
    ('Consumer Staples', 'Food Products', 'VDC', 'Proxy coverage: staples basket includes food products'),
    ('Consumer Staples', 'Food & Staples Retailing', 'KXI', 'Proxy coverage: global staples includes staples retail'),
    ('Consumer Staples', 'Household Products', 'VDC', 'Proxy coverage: staples basket includes household products'),
    ('Consumer Staples', 'Personal Care Products', 'VDC', 'Proxy coverage: staples basket includes personal care'),
    ('Consumer Staples', 'Tobacco', 'VDC', 'Proxy coverage: staples basket includes tobacco names'),

    ('Energy', 'Integrated Oil & Gas', 'XLE', 'Proxy coverage: sector ETF with integrated oil majors'),
    ('Energy', 'Renewable Energy', 'ICLN', 'Proxy coverage: renewable/clean energy ETF'),

    ('Financials', 'Banks', 'KBE', 'Proxy coverage: banking ETF'),

    ('Health Care', 'Biotechnology', 'XBI', 'Proxy coverage: biotech ETF'),
    ('Health Care', 'Life Sciences Tools & Services', 'XHE', 'Proxy coverage: closest listed healthcare equipment/tools ETF'),
    ('Health Care', 'Managed Care', 'IHF', 'Proxy coverage: healthcare providers includes managed care'),

    ('Indexes & Broad Market', 'Sector ETF', 'SPY', 'Proxy coverage: broad US equity market'),

    ('Industrials', 'Capital Goods', 'IYJ', 'Proxy coverage: US industrials ETF'),
    ('Industrials', 'Professional Services', 'IYJ', 'Proxy coverage: US industrials/professional exposure'),

    ('Information Technology', 'IT Services', 'IGM', 'Proxy coverage: expanded tech basket with IT services'),
    ('Information Technology', 'Networking', 'IGM', 'Proxy coverage: expanded tech basket with networking exposure'),

    ('International Equity', 'Sector ETF', 'EFA', 'Proxy coverage: developed markets ex-US'),

    ('Materials', 'Construction Materials', 'VAW', 'Proxy coverage: broad materials ETF'),
    ('Materials', 'Containers & Packaging', 'VAW', 'Proxy coverage: broad materials ETF'),
    ('Materials', 'Specialty Chemicals', 'VAW', 'Proxy coverage: broad materials ETF'),

    ('Real Estate', 'REITs - Data Centers', 'SRVR', 'Proxy coverage: data-center and infrastructure REITs'),
    ('Real Estate', 'REITs - Health Care', 'IYR', 'Proxy coverage: broad US REIT basket includes healthcare REITs'),
    ('Real Estate', 'REITs - Retail', 'IYR', 'Proxy coverage: broad US REIT basket includes retail REITs'),

    ('Thematic', 'Sector ETF', 'BOTZ', 'Proxy coverage: flagship thematic basket'),

    ('Utilities', 'Electric Utilities', 'XLU', 'Proxy coverage: broad utilities ETF'),
    ('Utilities', 'Gas Utilities', 'XLU', 'Proxy coverage: broad utilities ETF'),
    ('Utilities', 'Independent Power Producers', 'FAN', 'Proxy coverage: wind/independent power theme'),
    ('Utilities', 'Multi-Utilities', 'XLU', 'Proxy coverage: broad utilities ETF')
),
resolved AS (
  SELECT
    pm.ticker,
    ss.subsector_id,
    pm.notes
  FROM proxy_map pm
  JOIN instrument i
    ON i.ticker = pm.ticker
  JOIN sector s
    ON s.sector_name = pm.sector_name
  JOIN subsector ss
    ON ss.sector_id = s.sector_id
   AND ss.subsector_name = pm.subsector_name
)
INSERT INTO instrument_classification (ticker, subsector_id, is_primary, notes)
SELECT
  r.ticker,
  r.subsector_id,
  FALSE,
  r.notes
FROM resolved r
ON CONFLICT (ticker, subsector_id) DO UPDATE
SET
  notes = EXCLUDED.notes;

COMMIT;
