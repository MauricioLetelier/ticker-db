import os
import sys
import logging

import pandas as pd
import psycopg
import yfinance as yf
import yaml

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
CONFIG_PATH = os.path.join(BASE_DIR, "config.yaml")
LOGGER = logging.getLogger("bootstrap_history")


def load_config() -> dict:
    if not os.path.exists(CONFIG_PATH):
        raise FileNotFoundError(
            f"Missing config file: {CONFIG_PATH}. Copy config.yaml.example to config.yaml."
        )
    with open(CONFIG_PATH, "r") as f:
        return yaml.safe_load(f)


def get_conn(cfg: dict) -> psycopg.Connection:
    db = cfg["db"]
    db_pass = os.environ.get("DB_PASS")
    if not db_pass:
        raise RuntimeError("DB_PASS is required in the environment.")
    conn_str = (
        f"host={db['host']} port={db['port']} dbname={db['name']} "
        f"user={db['user']} password={db_pass}"
    )
    return psycopg.connect(conn_str)


def ensure_tables(conn: psycopg.Connection) -> None:
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


def upsert_1m(conn: psycopg.Connection, ticker: str, df: pd.DataFrame) -> None:
    if df is None or df.empty:
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
            _to_float(r["Open"]),
            _to_float(r["High"]),
            _to_float(r["Low"]),
            _to_float(r["Close"]),
            _to_int(r["Volume"]),
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


def configure_logging() -> None:
    log_level = os.getenv("LOG_LEVEL", "INFO").upper()
    logging.basicConfig(
        level=log_level,
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )


def main():
    configure_logging()
    if len(sys.argv) < 2:
        print("Usage: python bootstrap_history.py TICKER [PERIOD] [INTERVAL]")
        print("Example: python bootstrap_history.py AAPL 1y 1d")
        print("Example: python bootstrap_history.py AAPL 7d 1m")
        sys.exit(1)

    ticker = sys.argv[1].strip().upper()
    cfg = load_config()
    bcfg = cfg.get("bootstrap", {})

    default_period = str(bcfg.get("daily_period", "1y"))
    period = default_period
    interval = "1d"

    if len(sys.argv) >= 3:
        arg2 = sys.argv[2].strip()
        if arg2 in {"1d", "1m"}:
            interval = arg2
        else:
            period = arg2

    if len(sys.argv) >= 4:
        interval = sys.argv[3].strip()

    if interval not in {"1d", "1m"}:
        raise ValueError("INTERVAL must be one of: 1d, 1m")

    if interval == "1m" and period == default_period:
        period = str(bcfg.get("intraday_period", "7d"))

    df = yf.download(ticker, period=period, interval=interval, auto_adjust=False, progress=False)
    df = normalize_ohlcv(df, ticker)

    with get_conn(cfg) as conn:
        ensure_tables(conn)
        if interval == "1d":
            upsert_1d(conn, ticker, df)
            table_name = "prices_1d"
        else:
            upsert_1m(conn, ticker, df)
            table_name = "prices_1m"
        conn.commit()

    rows = 0 if df is None else len(df)
    LOGGER.info(
        "bootstrapped ticker=%s period=%s interval=%s rows=%s table=%s",
        ticker,
        period,
        interval,
        rows,
        table_name,
    )
    print(f"Bootstrapped {ticker} ({period}, {interval}): {rows} rows into {table_name}")


if __name__ == "__main__":
    main()
