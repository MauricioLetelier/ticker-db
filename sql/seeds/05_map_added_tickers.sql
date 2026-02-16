-- 05_map_added_tickers.sql
-- Adds newly-added config tickers to instrument + maps them to your existing subsectors.
-- Safe to re-run.

-- ------------------------------------------------------------
-- A) Insert instruments (minimal metadata; you can enrich later)
-- ------------------------------------------------------------
INSERT INTO instrument (ticker, instrument_name, instrument_type, provider, currency) VALUES
('VOX',  NULL,'ETF',NULL,'USD'),
('IYZ',  NULL,'ETF',NULL,'USD'),
('XTL',  NULL,'ETF',NULL,'USD'),
('XRT',  NULL,'ETF',NULL,'USD'),
('PEJ',  NULL,'ETF',NULL,'USD'),
('CARZ', NULL,'ETF',NULL,'USD'),
('FXD',  NULL,'ETF',NULL,'USD'),
('KXI',  NULL,'ETF',NULL,'USD'),
('FXG',  NULL,'ETF',NULL,'USD'),
('XES',  NULL,'ETF',NULL,'USD'),
('IEO',  NULL,'ETF',NULL,'USD'),
('CRAK', NULL,'ETF',NULL,'USD'),
('KCE',  NULL,'ETF',NULL,'USD'),
('IYG',  NULL,'ETF',NULL,'USD'),
('IPAY', NULL,'ETF',NULL,'USD'),
('XPH',  NULL,'ETF',NULL,'USD'),
('XHE',  NULL,'ETF',NULL,'USD'),
('XHS',  NULL,'ETF',NULL,'USD'),
('PPA',  NULL,'ETF',NULL,'USD'),
('IYJ',  NULL,'ETF',NULL,'USD'),
('PKB',  NULL,'ETF',NULL,'USD'),
('IGM',  NULL,'ETF',NULL,'USD'),
('XME',  NULL,'ETF',NULL,'USD'),
('RTM',  NULL,'ETF',NULL,'USD'),
('WOOD', NULL,'ETF',NULL,'USD'),
('PYZ',  NULL,'ETF',NULL,'USD'),
('REZ',  NULL,'ETF',NULL,'USD'),
('INDS', NULL,'ETF',NULL,'USD'),
('OFFI', NULL,'ETF',NULL,'USD'),
('PHO',  NULL,'ETF',NULL,'USD'),
('FAN',  NULL,'ETF',NULL,'USD')
ON CONFLICT (ticker) DO NOTHING;

-- ------------------------------------------------------------
-- B) Ensure some subsectors exist (in case they weren't created)
-- ------------------------------------------------------------
-- Communication Services subsectors already in your taxonomy; if not, ensure:
INSERT INTO subsector (sector_id, subsector_name)
SELECT s.sector_id, x.subsector_name
FROM sector s
JOIN (VALUES
('Wireless Telecom'),
('Integrated Telecom'),
('Media'),
('Entertainment')
) x(subsector_name) ON s.sector_name='Communication Services'
ON CONFLICT DO NOTHING;

-- Consumer Discretionary
INSERT INTO subsector (sector_id, subsector_name)
SELECT s.sector_id, x.subsector_name
FROM sector s
JOIN (VALUES
('Retail Discretionary'),
('Hotels Restaurants & Leisure'),
('Automobiles & Components')
) x(subsector_name) ON s.sector_name='Consumer Discretionary'
ON CONFLICT DO NOTHING;

-- Consumer Staples
INSERT INTO subsector (sector_id, subsector_name)
SELECT s.sector_id, x.subsector_name
FROM sector s
JOIN (VALUES
('Household Products'),
('Personal Care Products'),
('Food Products'),
('Beverages'),
('Food & Staples Retailing')
) x(subsector_name) ON s.sector_name='Consumer Staples'
ON CONFLICT DO NOTHING;

-- Energy
INSERT INTO subsector (sector_id, subsector_name)
SELECT s.sector_id, x.subsector_name
FROM sector s
JOIN (VALUES
('Oil & Gas Equipment & Services'),
('Exploration & Production'),
('Refining & Marketing')
) x(subsector_name) ON s.sector_name='Energy'
ON CONFLICT DO NOTHING;

-- Financials
INSERT INTO subsector (sector_id, subsector_name)
SELECT s.sector_id, x.subsector_name
FROM sector s
JOIN (VALUES
('Capital Markets'),
('Financial Services'),
('Consumer Finance')
) x(subsector_name) ON s.sector_name='Financials'
ON CONFLICT DO NOTHING;

-- Health Care
INSERT INTO subsector (sector_id, subsector_name)
SELECT s.sector_id, x.subsector_name
FROM sector s
JOIN (VALUES
('Pharmaceuticals'),
('Health Care Equipment'),
('Health Care Services')
) x(subsector_name) ON s.sector_name='Health Care'
ON CONFLICT DO NOTHING;

-- Industrials
INSERT INTO subsector (sector_id, subsector_name)
SELECT s.sector_id, x.subsector_name
FROM sector s
JOIN (VALUES
('Aerospace & Defense'),
('Machinery'),
('Building Products'),
('Capital Goods'),
('Professional Services')
) x(subsector_name) ON s.sector_name='Industrials'
ON CONFLICT DO NOTHING;

-- Information Technology
INSERT INTO subsector (sector_id, subsector_name)
SELECT s.sector_id, x.subsector_name
FROM sector s
JOIN (VALUES
('Hardware')
) x(subsector_name) ON s.sector_name='Information Technology'
ON CONFLICT DO NOTHING;

-- Materials
INSERT INTO subsector (sector_id, subsector_name)
SELECT s.sector_id, x.subsector_name
FROM sector s
JOIN (VALUES
('Metals & Mining'),
('Chemicals'),
('Paper & Forest Products')
) x(subsector_name) ON s.sector_name='Materials'
ON CONFLICT DO NOTHING;

-- Real Estate
INSERT INTO subsector (sector_id, subsector_name)
SELECT s.sector_id, x.subsector_name
FROM sector s
JOIN (VALUES
('REITs - Residential'),
('REITs - Industrial'),
('REITs - Office')
) x(subsector_name) ON s.sector_name='Real Estate'
ON CONFLICT DO NOTHING;

-- Utilities
INSERT INTO subsector (sector_id, subsector_name)
SELECT s.sector_id, x.subsector_name
FROM sector s
JOIN (VALUES
('Water Utilities'),
('Renewable Utilities')
) x(subsector_name) ON s.sector_name='Utilities'
ON CONFLICT DO NOTHING;

-- ------------------------------------------------------------
-- C) Primary mappings (instrument_classification)
-- ------------------------------------------------------------

-- Communication Services
INSERT INTO instrument_classification (ticker, subsector_id, is_primary, notes)
SELECT 'VOX', ss.subsector_id, TRUE, 'Comm Services broad'
FROM sector s JOIN subsector ss ON ss.sector_id=s.sector_id
WHERE s.sector_name='Communication Services' AND ss.subsector_name='Sector ETF'
ON CONFLICT DO NOTHING;

INSERT INTO instrument_classification (ticker, subsector_id, is_primary, notes)
SELECT 'IYZ', ss.subsector_id, TRUE, 'Telecom'
FROM sector s JOIN subsector ss ON ss.sector_id=s.sector_id
WHERE s.sector_name='Communication Services' AND ss.subsector_name='Integrated Telecom'
ON CONFLICT DO NOTHING;

INSERT INTO instrument_classification (ticker, subsector_id, is_primary, notes)
SELECT 'XTL', ss.subsector_id, TRUE, 'Telecom'
FROM sector s JOIN subsector ss ON ss.sector_id=s.sector_id
WHERE s.sector_name='Communication Services' AND ss.subsector_name='Wireless Telecom'
ON CONFLICT DO NOTHING;

-- Consumer Discretionary
INSERT INTO instrument_classification (ticker, subsector_id, is_primary, notes)
SELECT 'XRT', ss.subsector_id, TRUE, 'Retail'
FROM sector s JOIN subsector ss ON ss.sector_id=s.sector_id
WHERE s.sector_name='Consumer Discretionary' AND ss.subsector_name='Retail Discretionary'
ON CONFLICT DO NOTHING;

INSERT INTO instrument_classification (ticker, subsector_id, is_primary, notes)
SELECT 'PEJ', ss.subsector_id, TRUE, 'Leisure / travel'
FROM sector s JOIN subsector ss ON ss.sector_id=s.sector_id
WHERE s.sector_name='Consumer Discretionary' AND ss.subsector_name='Hotels Restaurants & Leisure'
ON CONFLICT DO NOTHING;

INSERT INTO instrument_classification (ticker, subsector_id, is_primary, notes)
SELECT 'CARZ', ss.subsector_id, TRUE, 'Autos'
FROM sector s JOIN subsector ss ON ss.sector_id=s.sector_id
WHERE s.sector_name='Consumer Discretionary' AND ss.subsector_name='Automobiles & Components'
ON CONFLICT DO NOTHING;

INSERT INTO instrument_classification (ticker, subsector_id, is_primary, notes)
SELECT 'FXD', ss.subsector_id, TRUE, 'Consumer Disc broad'
FROM sector s JOIN subsector ss ON ss.sector_id=s.sector_id
WHERE s.sector_name='Consumer Discretionary' AND ss.subsector_name='Sector ETF'
ON CONFLICT DO NOTHING;

-- Consumer Staples
INSERT INTO instrument_classification (ticker, subsector_id, is_primary, notes)
SELECT 'KXI', ss.subsector_id, TRUE, 'Staples broad'
FROM sector s JOIN subsector ss ON ss.sector_id=s.sector_id
WHERE s.sector_name='Consumer Staples' AND ss.subsector_name='Sector ETF'
ON CONFLICT DO NOTHING;

INSERT INTO instrument_classification (ticker, subsector_id, is_primary, notes)
SELECT 'FXG', ss.subsector_id, TRUE, 'Staples broad'
FROM sector s JOIN subsector ss ON ss.sector_id=s.sector_id
WHERE s.sector_name='Consumer Staples' AND ss.subsector_name='Sector ETF'
ON CONFLICT DO NOTHING;

-- Energy
INSERT INTO instrument_classification (ticker, subsector_id, is_primary, notes)
SELECT 'XES', ss.subsector_id, TRUE, 'Oil services'
FROM sector s JOIN subsector ss ON ss.sector_id=s.sector_id
WHERE s.sector_name='Energy' AND ss.subsector_name='Oil & Gas Equipment & Services'
ON CONFLICT DO NOTHING;

INSERT INTO instrument_classification (ticker, subsector_id, is_primary, notes)
SELECT 'IEO', ss.subsector_id, TRUE, 'E&P'
FROM sector s JOIN subsector ss ON ss.sector_id=s.sector_id
WHERE s.sector_name='Energy' AND ss.subsector_name='Exploration & Production'
ON CONFLICT DO NOTHING;

INSERT INTO instrument_classification (ticker, subsector_id, is_primary, notes)
SELECT 'CRAK', ss.subsector_id, TRUE, 'Refiners'
FROM sector s JOIN subsector ss ON ss.sector_id=s.sector_id
WHERE s.sector_name='Energy' AND ss.subsector_name='Refining & Marketing'
ON CONFLICT DO NOTHING;

-- Financials
INSERT INTO instrument_classification (ticker, subsector_id, is_primary, notes)
SELECT 'KCE', ss.subsector_id, TRUE, 'Capital Markets'
FROM sector s JOIN subsector ss ON ss.sector_id=s.sector_id
WHERE s.sector_name='Financials' AND ss.subsector_name='Capital Markets'
ON CONFLICT DO NOTHING;

INSERT INTO instrument_classification (ticker, subsector_id, is_primary, notes)
SELECT 'IYG', ss.subsector_id, TRUE, 'Financial services'
FROM sector s JOIN subsector ss ON ss.sector_id=s.sector_id
WHERE s.sector_name='Financials' AND ss.subsector_name='Financial Services'
ON CONFLICT DO NOTHING;

INSERT INTO instrument_classification (ticker, subsector_id, is_primary, notes)
SELECT 'IPAY', ss.subsector_id, TRUE, 'Payments / consumer finance proxy'
FROM sector s JOIN subsector ss ON ss.sector_id=s.sector_id
WHERE s.sector_name='Financials' AND ss.subsector_name='Consumer Finance'
ON CONFLICT DO NOTHING;

-- Health Care
INSERT INTO instrument_classification (ticker, subsector_id, is_primary, notes)
SELECT 'XPH', ss.subsector_id, TRUE, 'Pharma'
FROM sector s JOIN subsector ss ON ss.sector_id=s.sector_id
WHERE s.sector_name='Health Care' AND ss.subsector_name='Pharmaceuticals'
ON CONFLICT DO NOTHING;

INSERT INTO instrument_classification (ticker, subsector_id, is_primary, notes)
SELECT 'XHE', ss.subsector_id, TRUE, 'Equipment'
FROM sector s JOIN subsector ss ON ss.sector_id=s.sector_id
WHERE s.sector_name='Health Care' AND ss.subsector_name='Health Care Equipment'
ON CONFLICT DO NOTHING;

INSERT INTO instrument_classification (ticker, subsector_id, is_primary, notes)
SELECT 'XHS', ss.subsector_id, TRUE, 'Services'
FROM sector s JOIN subsector ss ON ss.sector_id=s.sector_id
WHERE s.sector_name='Health Care' AND ss.subsector_name='Health Care Services'
ON CONFLICT DO NOTHING;

-- Industrials
INSERT INTO instrument_classification (ticker, subsector_id, is_primary, notes)
SELECT 'PPA', ss.subsector_id, TRUE, 'A&D'
FROM sector s JOIN subsector ss ON ss.sector_id=s.sector_id
WHERE s.sector_name='Industrials' AND ss.subsector_name='Aerospace & Defense'
ON CONFLICT DO NOTHING;

INSERT INTO instrument_classification (ticker, subsector_id, is_primary, notes)
SELECT 'IYJ', ss.subsector_id, TRUE, 'Machinery / capital goods proxy'
FROM sector s JOIN subsector ss ON ss.sector_id=s.sector_id
WHERE s.sector_name='Industrials' AND ss.subsector_name='Machinery'
ON CONFLICT DO NOTHING;

INSERT INTO instrument_classification (ticker, subsector_id, is_primary, notes)
SELECT 'PKB', ss.subsector_id, TRUE, 'Building / construction proxy'
FROM sector s JOIN subsector ss ON ss.sector_id=s.sector_id
WHERE s.sector_name='Industrials' AND ss.subsector_name='Building Products'
ON CONFLICT DO NOTHING;

-- Information Technology
INSERT INTO instrument_classification (ticker, subsector_id, is_primary, notes)
SELECT 'IGM', ss.subsector_id, TRUE, 'Broad tech proxy (hardware-leaning)'
FROM sector s JOIN subsector ss ON ss.sector_id=s.sector_id
WHERE s.sector_name='Information Technology' AND ss.subsector_name='Hardware'
ON CONFLICT DO NOTHING;

-- Materials
INSERT INTO instrument_classification (ticker, subsector_id, is_primary, notes)
SELECT 'XME', ss.subsector_id, TRUE, 'Metals & Mining'
FROM sector s JOIN subsector ss ON ss.sector_id=s.sector_id
WHERE s.sector_name='Materials' AND ss.subsector_name='Metals & Mining'
ON CONFLICT DO NOTHING;

INSERT INTO instrument_classification (ticker, subsector_id, is_primary, notes)
SELECT 'RTM', ss.subsector_id, TRUE, 'Materials broad'
FROM sector s JOIN subsector ss ON ss.sector_id=s.sector_id
WHERE s.sector_name='Materials' AND ss.subsector_name='Sector ETF'
ON CONFLICT DO NOTHING;

INSERT INTO instrument_classification (ticker, subsector_id, is_primary, notes)
SELECT 'PYZ', ss.subsector_id, TRUE, 'Basic materials / chemicals proxy'
FROM sector s JOIN subsector ss ON ss.sector_id=s.sector_id
WHERE s.sector_name='Materials' AND ss.subsector_name='Chemicals'
ON CONFLICT DO NOTHING;

INSERT INTO instrument_classification (ticker, subsector_id, is_primary, notes)
SELECT 'WOOD', ss.subsector_id, TRUE, 'Forest products proxy'
FROM sector s JOIN subsector ss ON ss.sector_id=s.sector_id
WHERE s.sector_name='Materials' AND ss.subsector_name='Paper & Forest Products'
ON CONFLICT DO NOTHING;

-- Real Estate
INSERT INTO instrument_classification (ticker, subsector_id, is_primary, notes)
SELECT 'REZ', ss.subsector_id, TRUE, 'Residential REITs proxy'
FROM sector s JOIN subsector ss ON ss.sector_id=s.sector_id
WHERE s.sector_name='Real Estate' AND ss.subsector_name='REITs - Residential'
ON CONFLICT DO NOTHING;

INSERT INTO instrument_classification (ticker, subsector_id, is_primary, notes)
SELECT 'INDS', ss.subsector_id, TRUE, 'Industrial REITs proxy'
FROM sector s JOIN subsector ss ON ss.sector_id=s.sector_id
WHERE s.sector_name='Real Estate' AND ss.subsector_name='REITs - Industrial'
ON CONFLICT DO NOTHING;

INSERT INTO instrument_classification (ticker, subsector_id, is_primary, notes)
SELECT 'OFFI', ss.subsector_id, TRUE, 'Office REITs proxy'
FROM sector s JOIN subsector ss ON ss.sector_id=s.sector_id
WHERE s.sector_name='Real Estate' AND ss.subsector_name='REITs - Office'
ON CONFLICT DO NOTHING;

-- Utilities
INSERT INTO instrument_classification (ticker, subsector_id, is_primary, notes)
SELECT 'PHO', ss.subsector_id, TRUE, 'Water utilities proxy'
FROM sector s JOIN subsector ss ON ss.sector_id=s.sector_id
WHERE s.sector_name='Utilities' AND ss.subsector_name='Water Utilities'
ON CONFLICT DO NOTHING;

INSERT INTO instrument_classification (ticker, subsector_id, is_primary, notes)
SELECT 'FAN', ss.subsector_id, TRUE, 'Renewables / wind utilities proxy'
FROM sector s JOIN subsector ss ON ss.sector_id=s.sector_id
WHERE s.sector_name='Utilities' AND ss.subsector_name='Renewable Utilities'
ON CONFLICT DO NOTHING;

