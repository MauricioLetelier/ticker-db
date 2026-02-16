import os
import sys

import pandas as pd
import psycopg
import yfinance as yf
import yaml

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
CONFIG_PATH = os.path.join(BASE_DIR, "config.yaml")


def load_config() -> dict:
    with open(CONFIG_PATH, "r") as f:
        return yaml.safe_load(f)


def get_conn(cfg: dict) -> psycopg.Connection:
    db = cfg["db"]
    db_pass = os.environ["DB_PASS"]
    conn_str = (
        f"host={db['host']} port={db['port']} dbname={db['name']} "
        f"user={db['user']} password={db_pass}"
    )
    return psycopg.connect(conn_str)


def ensure_table(conn: psycopg.Connection) -> None:
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


def normalize_ohlcv(df: pd.DataFrame, ticker: str) -> pd.DataFrame:
    if df is None or df.empty:
        return df

    if isinstance(df.columns, pd.MultiIndex):
        # Most common yfinance format: (Field, Ticker) with ticker on last level
        if ticker in df.columns.get_level_values(-1):
            df = df.xs(ticker, axis=1, level=-1)
        elif ticker in df.columns.get_level_values(0):
            df = df.xs(ticker, axis=1, level=0)
        else:
            any_t = df.columns.get_level_values(-1)[0]
            df = df.xs(any_t, axis=1, level=-1)

    # Ensure expected columns exist
    for col in ["Open", "High", "Low", "Close", "Adj Close", "Volume"]:
        if col not in df.columns:
            df[col] = pd.NA
    return df


def _to_float(x):
    return float(x) if pd.notna(x) else None


def _to_int(x):
    return int(x) if pd.notna(x) else None


def upsert_1d(conn: psycopg.Connection, ticker: str, df: pd.DataFrame) -> None:
    if df is None or df.empty:
        return

    df = df.copy()
    df.index = pd.to_datetime(df.index).date

    rows = []
    for dt, r in df.iterrows():
        rows.append((
            ticker, dt,
            _to_float(r["Open"]),
            _to_float(r["High"]),
            _to_float(r["Low"]),
            _to_float(r["Close"]),
            _to_float(r["Adj Close"]),
            _to_int(r["Volume"]),
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
        print("Example: python bootstrap_history.py AAPL 1y")
        sys.exit(1)

    ticker = sys.argv[1].strip().upper()
    cfg = load_config()

    default_period = cfg.get("bootstrap", {}).get("daily_period", "1y")
    period = sys.argv[2].strip() if len(sys.argv) >= 3 else default_period

    df = yf.download(ticker, period=period, interval="1d", auto_adjust=False, progress=False)
    df = normalize_ohlcv(df, ticker)

    with get_conn(cfg) as conn:
        ensure_table(conn)
        upsert_1d(conn, ticker, df)
        conn.commit()

    print(f"Bootstrapped {ticker} ({period}): {len(df)} rows into prices_1d")


if __name__ == "__main__":
    main()
