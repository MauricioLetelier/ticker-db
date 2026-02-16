-- 04_seed_config_tickers.sql
-- Add all tickers from config.yaml to DB + primary classifications.
-- Safe to re-run (idempotent).

-- ------------------------------------------------------------
-- A) Add a few extra "meta" sectors for non-GICS universes
-- ------------------------------------------------------------
INSERT INTO sector (sector_name) VALUES
('Indexes & Broad Market'),
('International Equity'),
('Thematic'),
('Commodities')
ON CONFLICT DO NOTHING;

-- SubSectors for meta sectors
INSERT INTO subsector (sector_id, subsector_name)
SELECT s.sector_id, x.subsector_name
FROM sector s
JOIN (VALUES
('US Equity Index'),
('US Total Market'),
('US Large Cap'),
('US Small Cap'),
('US Tech Index')
) x(subsector_name) ON s.sector_name='Indexes & Broad Market'
ON CONFLICT DO NOTHING;

INSERT INTO subsector (sector_id, subsector_name)
SELECT s.sector_id, x.subsector_name
FROM sector s
JOIN (VALUES
('Developed ex-US'),
('Emerging Markets')
) x(subsector_name) ON s.sector_name='International Equity'
ON CONFLICT DO NOTHING;

INSERT INTO subsector (sector_id, subsector_name)
SELECT s.sector_id, x.subsector_name
FROM sector s
JOIN (VALUES
('Robotics & AI'),
('FinTech'),
('Cloud Computing'),
('Autonomous / EV'),
('Clean Energy'),
('Infrastructure'),
('Internet')
) x(subsector_name) ON s.sector_name='Thematic'
ON CONFLICT DO NOTHING;

INSERT INTO subsector (sector_id, subsector_name)
SELECT s.sector_id, x.subsector_name
FROM sector s
JOIN (VALUES
('Gold'),
('Gold Miners'),
('Steel / Metals'),
('Uranium')
) x(subsector_name) ON s.sector_name='Commodities'
ON CONFLICT DO NOTHING;

-- ------------------------------------------------------------
-- B) Ensure "Sector ETF" subsector exists for any GICS sector (already in your earlier seed)
-- ------------------------------------------------------------
INSERT INTO subsector (sector_id, subsector_name)
SELECT sector_id, 'Sector ETF'
FROM sector
ON CONFLICT DO NOTHING;

-- ------------------------------------------------------------
-- C) Insert instruments (your config universe)
-- ------------------------------------------------------------
INSERT INTO instrument (ticker, instrument_name, instrument_type, provider, currency) VALUES
-- Broad/Index
('SPY','SPDR S&P 500 ETF Trust','ETF','SPDR','USD'),
('QQQ','Invesco QQQ Trust','ETF','Invesco','USD'),
('DIA','SPDR Dow Jones Industrial Average ETF Trust','ETF','SPDR','USD'),
('IWM','iShares Russell 2000 ETF','ETF','iShares','USD'),
('VTI','Vanguard Total Stock Market ETF','ETF','Vanguard','USD'),

-- International
('EFA','iShares MSCI EAFE ETF','ETF','iShares','USD'),
('EEM','iShares MSCI Emerging Markets ETF','ETF','iShares','USD'),

-- Tech / Thematic
('BOTZ','Global X Robotics & Artificial Intelligence ETF','ETF','Global X','USD'),
('ROBO','ROBO Global Robotics and Automation Index ETF','ETF','ROBO','USD'),
('FINX','Global X FinTech ETF','ETF','Global X','USD'),
('CLOU','Global X Cloud Computing ETF','ETF','Global X','USD'),
('DRIV','Global X Autonomous & Electric Vehicles ETF','ETF','Global X','USD'),
('IDRV','iShares Self-Driving EV and Tech ETF','ETF','iShares','USD'),
('QCLN','First Trust NASDAQ Clean Edge Green Energy Index Fund','ETF','First Trust','USD'),
('ICLN','iShares Global Clean Energy ETF','ETF','iShares','USD'),
('TAN','Invesco Solar ETF','ETF','Invesco','USD'),
('FDN','First Trust Dow Jones Internet Index Fund','ETF','First Trust','USD'),
('PAVE','Global X U.S. Infrastructure Development ETF','ETF','Global X','USD'),

-- Health / Fin / Housing / RE / Travel
('IAI','iShares U.S. Broker-Dealers & Securities Exchanges ETF','ETF','iShares','USD'),
('KIE','SPDR S&P Insurance ETF','ETF','SPDR','USD'),
('IHE','iShares U.S. Pharmaceuticals ETF','ETF','iShares','USD'),
('ITB','iShares U.S. Home Construction ETF','ETF','iShares','USD'),
('XHB','SPDR S&P Homebuilders ETF','ETF','SPDR','USD'),
('IYR','iShares U.S. Real Estate ETF','ETF','iShares','USD'),
('REM','iShares Mortgage Real Estate ETF','ETF','iShares','USD'),
('JETS','U.S. Global Jets ETF','ETF','US Global','USD'),

-- Materials / Commodities
('GLD','SPDR Gold Shares','ETF','SPDR','USD'),
('GDX','VanEck Gold Miners ETF','ETF','VanEck','USD'),
('SLX','VanEck Steel ETF','ETF','VanEck','USD'),
('URA','Global X Uranium ETF','ETF','Global X','USD'),

-- Vanguard sector style
('VAW','Vanguard Materials ETF','ETF','Vanguard','USD'),
('VDC','Vanguard Consumer Staples ETF','ETF','Vanguard','USD')

ON CONFLICT (ticker) DO NOTHING;

-- ------------------------------------------------------------
-- D) Primary classifications (one primary subsector per ticker)
-- ------------------------------------------------------------

-- Broad/Index
INSERT INTO instrument_classification (ticker, subsector_id, is_primary, notes)
SELECT 'SPY', ss.subsector_id, TRUE, 'S&P 500 proxy'
FROM sector s JOIN subsector ss ON ss.sector_id=s.sector_id
WHERE s.sector_name='Indexes & Broad Market' AND ss.subsector_name='US Large Cap'
ON CONFLICT DO NOTHING;

INSERT INTO instrument_classification (ticker, subsector_id, is_primary, notes)
SELECT 'QQQ', ss.subsector_id, TRUE, 'NASDAQ-100 proxy'
FROM sector s JOIN subsector ss ON ss.sector_id=s.sector_id
WHERE s.sector_name='Indexes & Broad Market' AND ss.subsector_name='US Tech Index'
ON CONFLICT DO NOTHING;

INSERT INTO instrument_classification (ticker, subsector_id, is_primary, notes)
SELECT 'DIA', ss.subsector_id, TRUE, 'Dow 30 proxy'
FROM sector s JOIN subsector ss ON ss.sector_id=s.sector_id
WHERE s.sector_name='Indexes & Broad Market' AND ss.subsector_name='US Equity Index'
ON CONFLICT DO NOTHING;

INSERT INTO instrument_classification (ticker, subsector_id, is_primary, notes)
SELECT 'IWM', ss.subsector_id, TRUE, 'Russell 2000 proxy'
FROM sector s JOIN subsector ss ON ss.sector_id=s.sector_id
WHERE s.sector_name='Indexes & Broad Market' AND ss.subsector_name='US Small Cap'
ON CONFLICT DO NOTHING;

INSERT INTO instrument_classification (ticker, subsector_id, is_primary, notes)
SELECT 'VTI', ss.subsector_id, TRUE, 'Total US market'
FROM sector s JOIN subsector ss ON ss.sector_id=s.sector_id
WHERE s.sector_name='Indexes & Broad Market' AND ss.subsector_name='US Total Market'
ON CONFLICT DO NOTHING;

-- International
INSERT INTO instrument_classification (ticker, subsector_id, is_primary, notes)
SELECT 'EFA', ss.subsector_id, TRUE, 'Developed ex-US'
FROM sector s JOIN subsector ss ON ss.sector_id=s.sector_id
WHERE s.sector_name='International Equity' AND ss.subsector_name='Developed ex-US'
ON CONFLICT DO NOTHING;

INSERT INTO instrument_classification (ticker, subsector_id, is_primary, notes)
SELECT 'EEM', ss.subsector_id, TRUE, 'Emerging markets'
FROM sector s JOIN subsector ss ON ss.sector_id=s.sector_id
WHERE s.sector_name='International Equity' AND ss.subsector_name='Emerging Markets'
ON CONFLICT DO NOTHING;

-- Thematic
INSERT INTO instrument_classification (ticker, subsector_id, is_primary, notes)
SELECT 'BOTZ', ss.subsector_id, TRUE, 'Robotics & AI'
FROM sector s JOIN subsector ss ON ss.sector_id=s.sector_id
WHERE s.sector_name='Thematic' AND ss.subsector_name='Robotics & AI'
ON CONFLICT DO NOTHING;

INSERT INTO instrument_classification (ticker, subsector_id, is_primary, notes)
SELECT 'ROBO', ss.subsector_id, TRUE, 'Robotics & AI'
FROM sector s JOIN subsector ss ON ss.sector_id=s.sector_id
WHERE s.sector_name='Thematic' AND ss.subsector_name='Robotics & AI'
ON CONFLICT DO NOTHING;

INSERT INTO instrument_classification (ticker, subsector_id, is_primary, notes)
SELECT 'FINX', ss.subsector_id, TRUE, 'FinTech'
FROM sector s JOIN subsector ss ON ss.sector_id=s.sector_id
WHERE s.sector_name='Thematic' AND ss.subsector_name='FinTech'
ON CONFLICT DO NOTHING;

INSERT INTO instrument_classification (ticker, subsector_id, is_primary, notes)
SELECT 'CLOU', ss.subsector_id, TRUE, 'Cloud Computing'
FROM sector s JOIN subsector ss ON ss.sector_id=s.sector_id
WHERE s.sector_name='Thematic' AND ss.subsector_name='Cloud Computing'
ON CONFLICT DO NOTHING;

INSERT INTO instrument_classification (ticker, subsector_id, is_primary, notes)
SELECT 'DRIV', ss.subsector_id, TRUE, 'Autonomous / EV'
FROM sector s JOIN subsector ss ON ss.sector_id=s.sector_id
WHERE s.sector_name='Thematic' AND ss.subsector_name='Autonomous / EV'
ON CONFLICT DO NOTHING;

INSERT INTO instrument_classification (ticker, subsector_id, is_primary, notes)
SELECT 'IDRV', ss.subsector_id, TRUE, 'Autonomous / EV'
FROM sector s JOIN subsector ss ON ss.sector_id=s.sector_id
WHERE s.sector_name='Thematic' AND ss.subsector_name='Autonomous / EV'
ON CONFLICT DO NOTHING;

INSERT INTO instrument_classification (ticker, subsector_id, is_primary, notes)
SELECT 'ICLN', ss.subsector_id, TRUE, 'Clean Energy'
FROM sector s JOIN subsector ss ON ss.sector_id=s.sector_id
WHERE s.sector_name='Thematic' AND ss.subsector_name='Clean Energy'
ON CONFLICT DO NOTHING;

INSERT INTO instrument_classification (ticker, subsector_id, is_primary, notes)
SELECT 'QCLN', ss.subsector_id, TRUE, 'Clean Energy'
FROM sector s JOIN subsector ss ON ss.sector_id=s.sector_id
WHERE s.sector_name='Thematic' AND ss.subsector_name='Clean Energy'
ON CONFLICT DO NOTHING;

INSERT INTO instrument_classification (ticker, subsector_id, is_primary, notes)
SELECT 'TAN', ss.subsector_id, TRUE, 'Clean Energy (solar)'
FROM sector s JOIN subsector ss ON ss.sector_id=s.sector_id
WHERE s.sector_name='Thematic' AND ss.subsector_name='Clean Energy'
ON CONFLICT DO NOTHING;

INSERT INTO instrument_classification (ticker, subsector_id, is_primary, notes)
SELECT 'FDN', ss.subsector_id, TRUE, 'Internet'
FROM sector s JOIN subsector ss ON ss.sector_id=s.sector_id
WHERE s.sector_name='Thematic' AND ss.subsector_name='Internet'
ON CONFLICT DO NOTHING;

INSERT INTO instrument_classification (ticker, subsector_id, is_primary, notes)
SELECT 'PAVE', ss.subsector_id, TRUE, 'Infrastructure'
FROM sector s JOIN subsector ss ON ss.sector_id=s.sector_id
WHERE s.sector_name='Thematic' AND ss.subsector_name='Infrastructure'
ON CONFLICT DO NOTHING;

-- Finance / Health / Housing / RE / Transport (map into existing GICS subsectors you already created)
INSERT INTO instrument_classification (ticker, subsector_id, is_primary, notes)
SELECT 'IAI', ss.subsector_id, TRUE, 'Capital Markets'
FROM sector s JOIN subsector ss ON ss.sector_id=s.sector_id
WHERE s.sector_name='Financials' AND ss.subsector_name='Capital Markets'
ON CONFLICT DO NOTHING;

INSERT INTO instrument_classification (ticker, subsector_id, is_primary, notes)
SELECT 'KIE', ss.subsector_id, TRUE, 'Insurance'
FROM sector s JOIN subsector ss ON ss.sector_id=s.sector_id
WHERE s.sector_name='Financials' AND ss.subsector_name='Insurance'
ON CONFLICT DO NOTHING;

INSERT INTO instrument_classification (ticker, subsector_id, is_primary, notes)
SELECT 'IHE', ss.subsector_id, TRUE, 'Pharmaceuticals'
FROM sector s JOIN subsector ss ON ss.sector_id=s.sector_id
WHERE s.sector_name='Health Care' AND ss.subsector_name='Pharmaceuticals'
ON CONFLICT DO NOTHING;

INSERT INTO instrument_classification (ticker, subsector_id, is_primary, notes)
SELECT 'ITB', ss.subsector_id, TRUE, 'Homebuilders'
FROM sector s JOIN subsector ss ON ss.sector_id=s.sector_id
WHERE s.sector_name='Consumer Discretionary' AND ss.subsector_name='Homebuilders'
ON CONFLICT DO NOTHING;

INSERT INTO instrument_classification (ticker, subsector_id, is_primary, notes)
SELECT 'XHB', ss.subsector_id, TRUE, 'Homebuilders'
FROM sector s JOIN subsector ss ON ss.sector_id=s.sector_id
WHERE s.sector_name='Consumer Discretionary' AND ss.subsector_name='Homebuilders'
ON CONFLICT DO NOTHING;

INSERT INTO instrument_classification (ticker, subsector_id, is_primary, notes)
SELECT 'IYR', ss.subsector_id, TRUE, 'Real Estate sector ETF'
FROM sector s JOIN subsector ss ON ss.sector_id=s.sector_id
WHERE s.sector_name='Real Estate' AND ss.subsector_name='Sector ETF'
ON CONFLICT DO NOTHING;

INSERT INTO instrument_classification (ticker, subsector_id, is_primary, notes)
SELECT 'REM', ss.subsector_id, TRUE, 'Mortgage REITs'
FROM sector s JOIN subsector ss ON ss.sector_id=s.sector_id
WHERE s.sector_name='Financials' AND ss.subsector_name='REITs - Mortgage'
ON CONFLICT DO NOTHING;

INSERT INTO instrument_classification (ticker, subsector_id, is_primary, notes)
SELECT 'JETS', ss.subsector_id, TRUE, 'Airlines (transportation)'
FROM sector s JOIN subsector ss ON ss.sector_id=s.sector_id
WHERE s.sector_name='Industrials' AND ss.subsector_name='Transportation'
ON CONFLICT DO NOTHING;

-- Commodities
INSERT INTO instrument_classification (ticker, subsector_id, is_primary, notes)
SELECT 'GLD', ss.subsector_id, TRUE, 'Gold'
FROM sector s JOIN subsector ss ON ss.sector_id=s.sector_id
WHERE s.sector_name='Commodities' AND ss.subsector_name='Gold'
ON CONFLICT DO NOTHING;

INSERT INTO instrument_classification (ticker, subsector_id, is_primary, notes)
SELECT 'GDX', ss.subsector_id, TRUE, 'Gold Miners'
FROM sector s JOIN subsector ss ON ss.sector_id=s.sector_id
WHERE s.sector_name='Commodities' AND ss.subsector_name='Gold Miners'
ON CONFLICT DO NOTHING;

INSERT INTO instrument_classification (ticker, subsector_id, is_primary, notes)
SELECT 'SLX', ss.subsector_id, TRUE, 'Steel / Metals'
FROM sector s JOIN subsector ss ON ss.sector_id=s.sector_id
WHERE s.sector_name='Commodities' AND ss.subsector_name='Steel / Metals'
ON CONFLICT DO NOTHING;

INSERT INTO instrument_classification (ticker, subsector_id, is_primary, notes)
SELECT 'URA', ss.subsector_id, TRUE, 'Uranium'
FROM sector s JOIN subsector ss ON ss.sector_id=s.sector_id
WHERE s.sector_name='Commodities' AND ss.subsector_name='Uranium'
ON CONFLICT DO NOTHING;

-- Vanguard sector ETFs -> map to GICS "Sector ETF"
INSERT INTO instrument_classification (ticker, subsector_id, is_primary, notes)
SELECT 'VAW', ss.subsector_id, TRUE, 'Materials sector ETF (Vanguard)'
FROM sector s JOIN subsector ss ON ss.sector_id=s.sector_id
WHERE s.sector_name='Materials' AND ss.subsector_name='Sector ETF'
ON CONFLICT DO NOTHING;

INSERT INTO instrument_classification (ticker, subsector_id, is_primary, notes)
SELECT 'VDC', ss.subsector_id, TRUE, 'Consumer Staples sector ETF (Vanguard)'
FROM sector s JOIN subsector ss ON ss.sector_id=s.sector_id
WHERE s.sector_name='Consumer Staples' AND ss.subsector_name='Sector ETF'
ON CONFLICT DO NOTHING;

-- Existing tickers you already seeded earlier (keeps things consistent; no harm)
-- (No extra inserts needed here.)
