-- 02_seed_sectors_subsectors_instruments.sql
-- Inserts sectors/subsectors and maps ETFs.

INSERT INTO sector (sector_name) VALUES
('Communication Services'),
('Consumer Discretionary'),
('Consumer Staples'),
('Energy'),
('Financials'),
('Health Care'),
('Industrials'),
('Information Technology'),
('Materials'),
('Real Estate'),
('Utilities')
ON CONFLICT DO NOTHING;

-- Communication Services
INSERT INTO subsector (sector_id, subsector_name)
SELECT s.sector_id, x.subsector_name
FROM sector s
JOIN (VALUES
('Internet & Direct Marketing Retail'),
('Interactive Media & Services'),
('Media'),
('Entertainment'),
('Wireless Telecom'),
('Integrated Telecom')
) x(subsector_name) ON s.sector_name='Communication Services'
ON CONFLICT DO NOTHING;

-- Consumer Discretionary
INSERT INTO subsector (sector_id, subsector_name)
SELECT s.sector_id, x.subsector_name
FROM sector s
JOIN (VALUES
('Automobiles & Components'),
('Consumer Durables & Apparel'),
('Consumer Services'),
('Retail Discretionary'),
('Hotels Restaurants & Leisure'),
('Homebuilders')
) x(subsector_name) ON s.sector_name='Consumer Discretionary'
ON CONFLICT DO NOTHING;

-- Consumer Staples
INSERT INTO subsector (sector_id, subsector_name)
SELECT s.sector_id, x.subsector_name
FROM sector s
JOIN (VALUES
('Food & Staples Retailing'),
('Food Products'),
('Beverages'),
('Household Products'),
('Personal Care Products'),
('Tobacco')
) x(subsector_name) ON s.sector_name='Consumer Staples'
ON CONFLICT DO NOTHING;

-- Energy
INSERT INTO subsector (sector_id, subsector_name)
SELECT s.sector_id, x.subsector_name
FROM sector s
JOIN (VALUES
('Integrated Oil & Gas'),
('Exploration & Production'),
('Oil & Gas Equipment & Services'),
('Midstream / Pipelines'),
('Refining & Marketing'),
('Renewable Energy')
) x(subsector_name) ON s.sector_name='Energy'
ON CONFLICT DO NOTHING;

-- Financials
INSERT INTO subsector (sector_id, subsector_name)
SELECT s.sector_id, x.subsector_name
FROM sector s
JOIN (VALUES
('Banks'),
('Capital Markets'),
('Insurance'),
('Consumer Finance'),
('Financial Services'),
('REITs - Mortgage')
) x(subsector_name) ON s.sector_name='Financials'
ON CONFLICT DO NOTHING;

-- Health Care
INSERT INTO subsector (sector_id, subsector_name)
SELECT s.sector_id, x.subsector_name
FROM sector s
JOIN (VALUES
('Pharmaceuticals'),
('Biotechnology'),
('Health Care Equipment'),
('Health Care Services'),
('Life Sciences Tools & Services'),
('Managed Care')
) x(subsector_name) ON s.sector_name='Health Care'
ON CONFLICT DO NOTHING;

-- Industrials
INSERT INTO subsector (sector_id, subsector_name)
SELECT s.sector_id, x.subsector_name
FROM sector s
JOIN (VALUES
('Aerospace & Defense'),
('Capital Goods'),
('Professional Services'),
('Transportation'),
('Machinery'),
('Building Products')
) x(subsector_name) ON s.sector_name='Industrials'
ON CONFLICT DO NOTHING;

-- Information Technology
INSERT INTO subsector (sector_id, subsector_name)
SELECT s.sector_id, x.subsector_name
FROM sector s
JOIN (VALUES
('Semiconductors'),
('Software'),
('IT Services'),
('Hardware'),
('Networking'),
('Cybersecurity')
) x(subsector_name) ON s.sector_name='Information Technology'
ON CONFLICT DO NOTHING;

-- Materials
INSERT INTO subsector (sector_id, subsector_name)
SELECT s.sector_id, x.subsector_name
FROM sector s
JOIN (VALUES
('Chemicals'),
('Metals & Mining'),
('Construction Materials'),
('Paper & Forest Products'),
('Containers & Packaging'),
('Specialty Chemicals')
) x(subsector_name) ON s.sector_name='Materials'
ON CONFLICT DO NOTHING;

-- Real Estate
INSERT INTO subsector (sector_id, subsector_name)
SELECT s.sector_id, x.subsector_name
FROM sector s
JOIN (VALUES
('REITs - Industrial'),
('REITs - Residential'),
('REITs - Retail'),
('REITs - Office'),
('REITs - Data Centers'),
('REITs - Health Care')
) x(subsector_name) ON s.sector_name='Real Estate'
ON CONFLICT DO NOTHING;

-- Utilities
INSERT INTO subsector (sector_id, subsector_name)
SELECT s.sector_id, x.subsector_name
FROM sector s
JOIN (VALUES
('Electric Utilities'),
('Multi-Utilities'),
('Gas Utilities'),
('Water Utilities'),
('Independent Power Producers'),
('Renewable Utilities')
) x(subsector_name) ON s.sector_name='Utilities'
ON CONFLICT DO NOTHING;

-- Sector ETFs (SPDR Select Sector)
INSERT INTO instrument (ticker, instrument_name, instrument_type, provider, currency) VALUES
('XLC','Communication Services Select Sector SPDR Fund','ETF','SPDR','USD'),
('XLY','Consumer Discretionary Select Sector SPDR Fund','ETF','SPDR','USD'),
('XLP','Consumer Staples Select Sector SPDR Fund','ETF','SPDR','USD'),
('XLE','Energy Select Sector SPDR Fund','ETF','SPDR','USD'),
('XLF','Financial Select Sector SPDR Fund','ETF','SPDR','USD'),
('XLV','Health Care Select Sector SPDR Fund','ETF','SPDR','USD'),
('XLI','Industrial Select Sector SPDR Fund','ETF','SPDR','USD'),
('XLK','Technology Select Sector SPDR Fund','ETF','SPDR','USD'),
('XLB','Materials Select Sector SPDR Fund','ETF','SPDR','USD'),
('XLRE','Real Estate Select Sector SPDR Fund','ETF','SPDR','USD'),
('XLU','Utilities Select Sector SPDR Fund','ETF','SPDR','USD')
ON CONFLICT (ticker) DO NOTHING;

-- Subsector ETFs (common proxies)
INSERT INTO instrument (ticker, instrument_name, instrument_type, provider, currency) VALUES
('SOXX','iShares Semiconductor ETF','ETF','iShares','USD'),
('SMH','VanEck Semiconductor ETF','ETF','VanEck','USD'),
('IGV','iShares Expanded Tech-Software Sector ETF','ETF','iShares','USD'),
('SKYY','First Trust Cloud Computing ETF','ETF','First Trust','USD'),
('HACK','ETFMG Prime Cyber Security ETF','ETF','ETFMG','USD'),
('IYT','iShares Transportation Average ETF','ETF','iShares','USD'),
('ITA','iShares U.S. Aerospace & Defense ETF','ETF','iShares','USD'),
('KRE','SPDR S&P Regional Banking ETF','ETF','SPDR','USD'),
('KBE','SPDR S&P Bank ETF','ETF','SPDR','USD'),
('IAK','iShares U.S. Insurance ETF','ETF','iShares','USD'),
('XBI','SPDR S&P Biotech ETF','ETF','SPDR','USD'),
('IBB','iShares Biotechnology ETF','ETF','iShares','USD'),
('IHI','iShares U.S. Medical Devices ETF','ETF','iShares','USD'),
('IHF','iShares U.S. Healthcare Providers ETF','ETF','iShares','USD'),
('XOP','SPDR S&P Oil & Gas Exploration & Production ETF','ETF','SPDR','USD'),
('OIH','VanEck Oil Services ETF','ETF','VanEck','USD'),
('AMLP','Alerian MLP ETF','ETF','Alerian','USD'),
('VNQ','Vanguard Real Estate ETF','ETF','Vanguard','USD'),
('SRVR','Pacer Benchmark Data & Infrastructure Real Estate ETF','ETF','Pacer','USD')
ON CONFLICT (ticker) DO NOTHING;

-- Create a generic subsector in each sector called "Sector ETF"
INSERT INTO subsector (sector_id, subsector_name)
SELECT sector_id, 'Sector ETF'
FROM sector
ON CONFLICT DO NOTHING;

-- Map XL* tickers to their sector's "Sector ETF"
INSERT INTO instrument_classification (ticker, subsector_id, is_primary, notes)
SELECT i.ticker, ss.subsector_id, TRUE, 'Primary sector ETF'
FROM instrument i
JOIN sector s ON (
  (i.ticker='XLC'  AND s.sector_name='Communication Services') OR
  (i.ticker='XLY'  AND s.sector_name='Consumer Discretionary') OR
  (i.ticker='XLP'  AND s.sector_name='Consumer Staples') OR
  (i.ticker='XLE'  AND s.sector_name='Energy') OR
  (i.ticker='XLF'  AND s.sector_name='Financials') OR
  (i.ticker='XLV'  AND s.sector_name='Health Care') OR
  (i.ticker='XLI'  AND s.sector_name='Industrials') OR
  (i.ticker='XLK'  AND s.sector_name='Information Technology') OR
  (i.ticker='XLB'  AND s.sector_name='Materials') OR
  (i.ticker='XLRE' AND s.sector_name='Real Estate') OR
  (i.ticker='XLU'  AND s.sector_name='Utilities')
)
JOIN subsector ss ON ss.sector_id=s.sector_id AND ss.subsector_name='Sector ETF'
ON CONFLICT DO NOTHING;

-- Subsector mappings (examples)
INSERT INTO instrument_classification (ticker, subsector_id, is_primary, notes)
SELECT 'SOXX', ss.subsector_id, TRUE, 'Semiconductors' FROM sector s
JOIN subsector ss ON ss.sector_id=s.sector_id AND ss.subsector_name='Semiconductors'
WHERE s.sector_name='Information Technology'
ON CONFLICT DO NOTHING;

INSERT INTO instrument_classification (ticker, subsector_id, is_primary, notes)
SELECT 'SMH', ss.subsector_id, FALSE, 'Semiconductors alt' FROM sector s
JOIN subsector ss ON ss.sector_id=s.sector_id AND ss.subsector_name='Semiconductors'
WHERE s.sector_name='Information Technology'
ON CONFLICT DO NOTHING;

INSERT INTO instrument_classification (ticker, subsector_id, is_primary, notes)
SELECT 'IGV', ss.subsector_id, TRUE, 'Software' FROM sector s
JOIN subsector ss ON ss.sector_id=s.sector_id AND ss.subsector_name='Software'
WHERE s.sector_name='Information Technology'
ON CONFLICT DO NOTHING;

INSERT INTO instrument_classification (ticker, subsector_id, is_primary, notes)
SELECT 'HACK', ss.subsector_id, TRUE, 'Cybersecurity' FROM sector s
JOIN subsector ss ON ss.sector_id=s.sector_id AND ss.subsector_name='Cybersecurity'
WHERE s.sector_name='Information Technology'
ON CONFLICT DO NOTHING;
