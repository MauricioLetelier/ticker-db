📊 Ticker DB – Momentum Investing Backend

Lightweight financial data backend running on:

🐘 PostgreSQL (Docker)

🐍 Python updater (yfinance + psycopg)

📈 Streamlit dashboard

📊 Metabase (optional analytics layer)

🧱 Architecture Overview
/opt/ticker-db
│
├── docker-compose.yml
├── README.md
│
├── sql/
│   ├── 01_schema_prices.sql
│   ├── 01_schema_classification.sql
│   ├── 02_seed_sectors.sql
│   └── 03_views.sql
│
├── updater/
│   ├── .venv/
│   ├── bootstrap_history.py
│   ├── daily_update.py
│   ├── config.yaml
│   └── requirements.txt
│
└── dashboard/
    └── app.py

🗄 Database Tables
1️⃣ prices_1d

Daily OHLCV data.

ticker TEXT
dt DATE
open NUMERIC
high NUMERIC
low NUMERIC
close NUMERIC
adj_close NUMERIC
volume BIGINT

PRIMARY KEY (ticker, dt)

2️⃣ prices_1m

1-minute intraday data.

PRIMARY KEY (ticker, dt)


⚠ Yahoo limitation:

~7 days of 1m

~60 days of 5m

3️⃣ Classification Tables

sector

subsector

instrument

instrument_classification

Used by dashboard filtering.

⚙️ Setup Instructions
Activate Environment
cd /opt/ticker-db/updater
source .venv/bin/activate

Set DB Password (Required)
export DB_PASS='StrongPassword123'


Your scripts require this environment variable.

🚀 Data Population
1️⃣ Bootstrap Single Ticker (Daily)
python bootstrap_history.py AAPL 1y


Pulls 1 year of daily data into prices_1d.

2️⃣ Bootstrap Minute Data
python bootstrap_history.py AAPL 7d 1m


Pulls 7 days of 1-minute data into prices_1m.

3️⃣ Full Bootstrap From config.yaml (1 Year)
cd /opt/ticker-db/updater
source .venv/bin/activate
export DB_PASS='StrongPassword123'

python -u run_full_bootstrap.py


If using inline runner:

python bootstrap_history.py XLC 1y


Or batch loop using config.

4️⃣ Daily Incremental Update

Updates only missing recent data:

python daily_update.py


Recommended to run via cron.

🔁 Reset Data
Delete Daily Data
docker exec -it postgres psql -U appuser -d appdb -c "TRUNCATE prices_1d;"

Delete Minute Data
docker exec -it postgres psql -U appuser -d appdb -c "TRUNCATE prices_1m;"

🔍 Verify Data

Enter database:

docker exec -it postgres psql -U appuser -d appdb


Check tables:

\dt


Count rows:

SELECT ticker, COUNT(*)
FROM prices_1d
GROUP BY ticker
ORDER BY 2 DESC;


Latest data:

SELECT *
FROM prices_1d
ORDER BY dt DESC
LIMIT 5;


Exit:

\q

📈 Run Dashboard

Container already exposes:

http://<YOUR_DROPLET_IP>:8501


Restart dashboard:

docker restart dashboard


Logs:

docker logs -f dashboard

🛡 Data Integrity

Recommended constraints:

PRIMARY KEY (ticker, dt)


Ensures:

No duplicate rows

Safe re-runs

Idempotent updates

🧠 config.yaml Structure
db:
  host: 127.0.0.1
  port: 5432
  name: appdb
  user: appuser

tickers:
  - SPY
  - QQQ
  - XLF
  - SMH

intervals:
  - 1d
  - 1m

⏰ Production Cron Example

Hourly incremental update:

crontab -e


Add:

0 * * * * cd /opt/ticker-db/updater && /opt/ticker-db/updater/.venv/bin/python daily_update.py >> updater.log 2>&1

🧹 Clean Rebuild Procedure

1️⃣ Truncate tables
2️⃣ Bootstrap 1 year
3️⃣ Verify row counts
4️⃣ Restart dashboard

🔥 Common Errors
KeyError: DB_PASS

Fix:

export DB_PASS='StrongPassword123'

relation "prices_1d" does not exist

You didn’t run schema SQL.

Duplicate data

Add:

PRIMARY KEY (ticker, dt)
