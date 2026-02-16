-- 01_schema_classification.sql
-- Dimensions + instrument mapping

CREATE TABLE IF NOT EXISTS sector (
  sector_id   SERIAL PRIMARY KEY,
  sector_name TEXT NOT NULL UNIQUE
);

CREATE TABLE IF NOT EXISTS subsector (
  subsector_id   SERIAL PRIMARY KEY,
  sector_id      INT NOT NULL REFERENCES sector(sector_id) ON DELETE CASCADE,
  subsector_name TEXT NOT NULL,
  UNIQUE (sector_id, subsector_name)
);

CREATE TABLE IF NOT EXISTS instrument (
  ticker           TEXT PRIMARY KEY,
  instrument_name  TEXT,
  instrument_type  TEXT NOT NULL DEFAULT 'ETF',  -- ETF, STOCK, INDEX, CRYPTO...
  provider         TEXT,
  currency         TEXT DEFAULT 'USD'
);

CREATE TABLE IF NOT EXISTS instrument_classification (
  ticker        TEXT NOT NULL REFERENCES instrument(ticker) ON DELETE CASCADE,
  subsector_id  INT  NOT NULL REFERENCES subsector(subsector_id) ON DELETE CASCADE,
  is_primary    BOOLEAN NOT NULL DEFAULT TRUE,
  notes         TEXT,
  PRIMARY KEY (ticker, subsector_id)
);

CREATE INDEX IF NOT EXISTS idx_subsector_sector_id ON subsector(sector_id);
CREATE INDEX IF NOT EXISTS idx_ic_subsector_id ON instrument_classification(subsector_id);
CREATE INDEX IF NOT EXISTS idx_ic_ticker ON instrument_classification(ticker);

-- Optional: only one primary subsector per ticker
CREATE UNIQUE INDEX IF NOT EXISTS uq_one_primary_subsector_per_ticker
ON instrument_classification(ticker)
WHERE is_primary = TRUE;
