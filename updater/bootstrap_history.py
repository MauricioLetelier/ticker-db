import os
import sys
import pandas as pd
import yfinance as yf
import psycopg
import yaml

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
CONFIG_PATH = os.path.join(BASE_DIR, "config.yaml")

def load_config():
    with open(CONFIG_PATH, "r") as f:
        return yaml.safe_load(f)

def get_conn(cfg):
    db = cfg["db"]
    db_pass = os.environ["DB_PASS"]
    conn_str = (
        f"host={db['host']} port={db['port']} dbname={db['name']} "
        f"user={db['user']} password={db_pass}"
    )
    return psycopg.connect(conn_str)

def ensure_table(conn):
    with conn.cursor() as cur:
        cur.execute("""
        CREATE TABLE IF NOT EXISTS prices_1d (
          ticker text NOT NULL,
          dt date NOT NULL,
          open double precision,
          high double precision,
          low double precision,
          close double precision,
          adj_close double precision,
          volume bigint,
          PRIMARY KEY (ticker, dt)
        );
        """)

def upsert_1d(conn, ticker: str, df: pd.DataFrame):
    if df.empty:
        return
    df = df.copy()
    df.index = pd.to_datetime(df.index).date

    rows = []
    for dt, r in df.iterrows():
        rows.append((
            ticker, dt,
            float(r.get("Open")) if pd.notna(r.get("Open")) else None,
            float(r.get("High")) if pd.notna(r.get("High")) else None,
            float(r.get("Low")) if pd.notna(r.get("Low")) else None,
            float(r.get("Close")) if pd.notna(r.get("Close")) else None,
            float(r.get("Adj Close")) if pd.notna(r.get("Adj Close")) else None,
            int(r.get("Volume")) if pd.notna(r.get("Volume")) else None,
        ))

    with conn.cursor() as cur:
        cur.executemany("""
            INSERT INTO prices_1d (ticker, dt, open, high, low, close, adj_close, volume)
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s)
            ON CONFLICT (ticker, dt) DO UPDATE SET
              open=EXCLUDED.open,
              high=EXCLUDED.high,
              low=EXCLUDED.low,
              close=EXCLUDED.close,
              adj_close=EXCLUDED.adj_close,
              volume=EXCLUDED.volume;
        """, rows)

def main():
    if len(sys.argv) < 2:
        print("Usage: python bootstrap_history.py TICKER [PERIOD]")
        sys.exit(1)

    ticker = sys.argv[1].strip().upper()
    cfg = load_config()

    default_period = cfg.get("bootstrap", {}).get("daily_period", "1y")
    period = sys.argv[2].strip() if len(sys.argv) >= 3 else default_period

    df = yf.download(ticker, period=period, interval="1d", auto_adjust=False, progress=False)

    with get_conn(cfg) as conn:
        ensure_table(conn)
        upsert_1d(conn, ticker, df)
        conn.commit()

    print(f"Bootstrapped {ticker} ({period}): {len(df)} rows into prices_1d")

if __name__ == "__main__":
    main()
