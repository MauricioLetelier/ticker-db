import os
import logging
import sys
from datetime import datetime, timedelta, timezone

import pandas as pd
import psycopg
import yfinance as yf
import yaml

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
CONFIG_PATH = os.path.join(BASE_DIR, "config.yaml")
LOGGER = logging.getLogger("daily_update")


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
    """
    yfinance sometimes returns MultiIndex columns like (Field, Ticker) even for one ticker.
    This converts it into single-level columns: Open/High/Low/Close/Adj Close/Volume.
    """
    if df is None or df.empty:
        return df

    if isinstance(df.columns, pd.MultiIndex):
        # Most common: (Field, Ticker) with ticker on last level
        if ticker in df.columns.get_level_values(-1):
            df = df.xs(ticker, axis=1, level=-1)
        elif ticker in df.columns.get_level_values(0):
            df = df.xs(ticker, axis=1, level=0)
        else:
            # fallback: use first ticker slice
            any_t = df.columns.get_level_values(-1)[0]
            df = df.xs(any_t, axis=1, level=-1)

    # Ensure expected columns exist (Adj Close may be absent)
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
    df.index = pd.to_datetime(df.index).date  # date index

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

    # Normalize to UTC timestamptz
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


def cleanup_1m(conn: psycopg.Connection, keep_hours: int) -> None:
    cutoff = datetime.now(timezone.utc) - timedelta(hours=keep_hours)
    with conn.cursor() as cur:
        cur.execute("DELETE FROM prices_1m WHERE ts < %s;", (cutoff,))


def configure_logging() -> None:
    log_level = os.getenv("LOG_LEVEL", "INFO").upper()
    logging.basicConfig(
        level=log_level,
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )


def main() -> int:
    configure_logging()
    cfg = load_config()
    tickers = [t.strip().upper() for t in cfg.get("tickers", []) if t and str(t).strip()]
    ucfg = cfg.get("update", {})
    if not tickers:
        raise RuntimeError("No tickers configured in config.yaml.")

    lookback = int(ucfg.get("daily_lookback_days", 14))
    intraday_enabled = bool(ucfg.get("intraday_enabled", True))
    intraday_period = str(ucfg.get("intraday_period", "1d"))
    keep_1m = int(ucfg.get("keep_1m_hours", 30))

    start_dt = (datetime.now(timezone.utc) - timedelta(days=lookback)).date().isoformat()
    success_count = 0
    failed_tickers = []

    with get_conn(cfg) as conn:
        ensure_tables(conn)

        for t in tickers:
            try:
                df1d = yf.download(t, start=start_dt, interval="1d", auto_adjust=False, progress=False)
                df1d = normalize_ohlcv(df1d, t)
                upsert_1d(conn, t, df1d)

                intraday_rows = 0
                if intraday_enabled:
                    df1m = yf.download(t, period=intraday_period, interval="1m", auto_adjust=False, progress=False)
                    df1m = normalize_ohlcv(df1m, t)
                    intraday_rows = 0 if df1m is None else len(df1m)
                    upsert_1m(conn, t, df1m)

                conn.commit()
                success_count += 1
                LOGGER.info(
                    "updated ticker=%s daily_rows=%s intraday_rows=%s",
                    t,
                    0 if df1d is None else len(df1d),
                    intraday_rows,
                )
            except Exception:
                conn.rollback()
                failed_tickers.append(t)
                LOGGER.exception("failed updating ticker=%s", t)

        if intraday_enabled:
            try:
                cleanup_1m(conn, keep_1m)
                conn.commit()
            except Exception:
                conn.rollback()
                LOGGER.exception("failed intraday cleanup")
                return 1

    LOGGER.info(
        "run complete total=%s success=%s failed=%s",
        len(tickers),
        success_count,
        len(failed_tickers),
    )
    if failed_tickers:
        LOGGER.warning("failed tickers: %s", ",".join(failed_tickers))

    if success_count == 0:
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
