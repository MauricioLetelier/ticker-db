# Ticker DB

Lightweight market-data backend:

- Postgres in Docker
- Python updater (`yfinance` + `psycopg`)
- Streamlit dashboard
- Optional Metabase

## Project Layout

```text
/opt/ticker-db
├── docker-compose.yml
├── .env.example
├── sql/
│   ├── 01_schema_classification.sql
│   ├── 03_views.sql
│   └── seeds/
├── updater/
│   ├── bootstrap_history.py
│   ├── daily_update.py
│   ├── run_daily_update.sh
│   ├── config.yaml.example
│   └── requirements.txt
└── dashboard/
    ├── app.py
    └── requirements.txt
```

## Main Tables

- `prices_1d`: daily OHLCV, primary key `(ticker, dt)`
- `prices_1m`: 1-minute OHLCV, primary key `(ticker, ts)`
- Classification tables: `sector`, `subsector`, `instrument`, `instrument_classification`

Notes on Yahoo intraday limits:

- `1m`: around 7 days
- short intraday intervals: around 60 days depending on interval/provider behavior

## 1) Initial Setup

1. Configure Compose secrets:

```bash
cd /opt/ticker-db
cp .env.example .env
# edit .env and set a strong POSTGRES_PASSWORD
```

2. Start services:

```bash
docker compose up -d postgres dashboard metabase
```

3. Install updater dependencies:

```bash
cd /opt/ticker-db/updater
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
cp config.yaml.example config.yaml
```

4. Set updater DB password:

```bash
export DB_PASS='replace_with_same_postgres_password'
```

## 2) Create Classification Schema (Optional but recommended for dashboard filters)

```bash
cd /opt/ticker-db
set -a
source .env
set +a
docker exec -i postgres psql -U "$POSTGRES_USER" -d "$POSTGRES_DB" < sql/01_schema_classification.sql
docker exec -i postgres psql -U "$POSTGRES_USER" -d "$POSTGRES_DB" < sql/seeds/02_seed_sectors_subsectors_instruments.sql
docker exec -i postgres psql -U "$POSTGRES_USER" -d "$POSTGRES_DB" < sql/seeds/04_seed_config_tickers.sql
docker exec -i postgres psql -U "$POSTGRES_USER" -d "$POSTGRES_DB" < sql/seeds/05_map_added_tickers.sql
docker exec -i postgres psql -U "$POSTGRES_USER" -d "$POSTGRES_DB" < sql/seeds/06_research_based_etf_mappings.sql
docker exec -i postgres psql -U "$POSTGRES_USER" -d "$POSTGRES_DB" < sql/seeds/07_research_proxy_subsector_coverage.sql
docker exec -i postgres psql -U "$POSTGRES_USER" -d "$POSTGRES_DB" < sql/03_views.sql
```

`06_research_based_etf_mappings.sql` contains curated primary mappings for specific ETFs and intentionally avoids generic fallback assignments.
`07_research_proxy_subsector_coverage.sql` adds non-primary proxy mappings so subsectors do not remain empty.

## 3) Backfill Data

Daily backfill:

```bash
cd /opt/ticker-db/updater
source .venv/bin/activate
export DB_PASS='replace_with_same_postgres_password'
python bootstrap_history.py AAPL 1y 1d
```

Minute backfill:

```bash
python bootstrap_history.py AAPL 7d 1m
```

## 4) Incremental Updates

Run once manually:

```bash
cd /opt/ticker-db/updater
source .venv/bin/activate
export DB_PASS='replace_with_same_postgres_password'
python daily_update.py
```

`daily_update.py` is idempotent (`ON CONFLICT ... DO UPDATE`) and now logs per ticker. Failed tickers are skipped so one bad symbol does not abort the entire run.

## 5) Production Cron (Droplet)

Create updater env file:

```bash
cd /opt/ticker-db/updater
cat > .env <<'EOF'
DB_PASS='replace_with_same_postgres_password'
EOF
chmod 600 .env
```

Create log directory:

```bash
sudo mkdir -p /var/log/ticker-db
sudo chown "$USER":"$USER" /var/log/ticker-db
```

Edit crontab:

```bash
crontab -e
```

Recommended entries:

```cron
SHELL=/bin/bash
PATH=/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin
CRON_TZ=Etc/UTC

# Hourly, weekdays only; lock prevents overlapping runs
7 * * * 1-5 /usr/bin/flock -n /tmp/ticker-db-update.lock /opt/ticker-db/updater/run_daily_update.sh >> /var/log/ticker-db/daily_update.log 2>&1

# Nightly maintenance
20 1 * * * source /opt/ticker-db/.env && /usr/bin/docker exec postgres psql -U "$POSTGRES_USER" -d "$POSTGRES_DB" -c "ANALYZE prices_1d; ANALYZE prices_1m;" >> /var/log/ticker-db/maintenance.log 2>&1
```

## Verification

```bash
cd /opt/ticker-db
set -a
source .env
set +a
docker exec -it postgres psql -U "$POSTGRES_USER" -d "$POSTGRES_DB"
```

```sql
SELECT ticker, COUNT(*) FROM prices_1d GROUP BY ticker ORDER BY 2 DESC;
SELECT ticker, MAX(dt) AS latest_dt FROM prices_1d GROUP BY ticker ORDER BY ticker;
SELECT COUNT(*) FROM prices_1m;
```

## Dashboard

- URL: `http://<droplet_ip>:8501`
- Logs: `docker logs -f dashboard`

## Common Issues

- `DB_PASS is required in the environment`:
  - export `DB_PASS` or set `updater/.env` when using `run_daily_update.sh`.
- `set POSTGRES_PASSWORD in .env` when running Compose:
  - create `.env` from `.env.example`.
- Empty intraday data:
  - expected outside market hours, on market holidays, or beyond Yahoo retention windows.
