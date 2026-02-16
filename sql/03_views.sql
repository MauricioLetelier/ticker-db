-- 03_views.sql

CREATE OR REPLACE VIEW v_instrument_hierarchy AS
SELECT
  s.sector_name,
  ss.subsector_name,
  i.ticker,
  i.instrument_name,
  i.instrument_type,
  i.provider,
  i.currency,
  ic.is_primary,
  ic.notes
FROM instrument i
JOIN instrument_classification ic ON ic.ticker = i.ticker
JOIN subsector ss ON ss.subsector_id = ic.subsector_id
JOIN sector s ON s.sector_id = ss.sector_id
ORDER BY s.sector_name, ss.subsector_name, i.ticker;

-- Optional: ONLY create this if you have a prices_1d table
DO $$
BEGIN
  IF EXISTS (
    SELECT 1
    FROM information_schema.tables
    WHERE table_schema='public' AND table_name='prices_1d'
  ) THEN
    EXECUTE $v$
      CREATE OR REPLACE VIEW v_prices_1d_classified AS
      SELECT
        p.date,
        s.sector_name,
        ss.subsector_name,
        p.ticker,
        p.open,
        p.high,
        p.low,
        p.close,
        p.adj_close,
        p.volume
      FROM prices_1d p
      JOIN instrument_classification ic ON ic.ticker = p.ticker AND ic.is_primary = TRUE
      JOIN subsector ss ON ss.subsector_id = ic.subsector_id
      JOIN sector s ON s.sector_id = ss.sector_id
    $v$;
  END IF;
END $$;
