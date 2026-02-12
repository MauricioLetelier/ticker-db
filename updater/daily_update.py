import os
from datetime import datetime, timedelta, timezone
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

def ensure_tables(conn):
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
        cur.execute("""
        CREATE TABLE IF NOT EXISTS prices_1m (
          ticker text NOT NULL,
          ts timestamptz NOT NULL,
          open double precision,
          high double precision,
          low double precision,
          close double precision,
          volume bigint,
          PRIMARY KEY (ticker, ts)
        );
        """)
        cur.execute("CREATE INDEX IF NOT EXISTS prices_1m_ts_idx ON prices_1m (ts);")

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

def upsert_1m(conn, ticker: str, df: pd.DataFrame):
    if df.empty:
        return
    df = df.copy()
    idx = pd.to_datetime(df.index)
    if idx.tz is None:
        idx = idx.tz_localize("UTC")
    else:
        idx = idx.tz_convert("UTC")
    df.index = idx

    rows = []
    for ts, r in df.iterrows():
        rows.append((
            ticker, ts.to_pydatetime(),
            float(r.get("Open")) if pd.notna(r.get("Open")) else None,
            float(r.get("High")) if pd.notna(r.get("High")) else None,
            float(r.get("Low")) if pd.notna(r.get("Low")) else None,
            float(r.get("Close")) if pd.notna(r.get("Close")) else None,
            int(r.get("Volume")) if pd.notna(r.get("Volume")) else None,
        ))

    with conn.cursor() as cur:
        cur.executemany("""
            INSERT INTO prices_1m (ticker, ts, open, high, low, close, volume)
            VALUES (%s,%s,%s,%s,%s,%s,%s)
            ON CONFLICT (ticker, ts) DO UPDATE SET
              open=EXCLUDED.open,
              high=EXCLUDED.high,
              low=EXCLUDED.low,
              close=EXCLUDED.close,
              volume=EXCLUDED.volume;
        """, rows)

def cleanup_1m(conn, keep_hours: int):
    cutoff = datetime.now(timezone.utc) - timedelta(hours=keep_hours)
    with conn.cursor() as cur:
        cur.execute("DELETE FROM prices_1m WHERE ts < %s;", (cutoff,))

def main():
    cfg = load_config()
    tickers = [t.strip().upper() for t in cfg["tickers"] if t.strip()]
    ucfg = cfg["update"]

    lookback = int(ucfg["daily_lookback_days"])
    intraday_enabled = bool(ucfg.get("intraday_enabled", True))
    intraday_period = str(ucfg.get("intraday_period", "1d"))
    keep_1m = int(ucfg.get("keep_1m_hours", 30))

    start_dt = (datetime.now(timezone.utc) - timedelta(days=lookback)).date().isoformat()

    with get_conn(cfg) as conn:
        ensure_tables(conn)

        for t in tickers:
            df1d = yf.download(t, start=start_dt, interval="1d", auto_adjust=False, progress=False)
            upsert_1d(conn, t, df1d)

            if intraday_enabled:
                df1m = yf.download(t, period=intraday_period, interval="1m", auto_adjust=False, progress=False)
                upsert_1m(conn, t, df1m)

        if intraday_enabled:
            cleanup_1m(conn, keep_1m)

        conn.commit()

if __name__ == "__main__":
    main()
