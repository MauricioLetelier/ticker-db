import os
import logging
import sys
from datetime import date, datetime, timedelta, timezone
from time import monotonic

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


def get_latest_1d_dt_map(conn: psycopg.Connection, tickers: list[str]) -> dict[str, date]:
    if not tickers:
        return {}
    with conn.cursor() as cur:
        # Single indexed pass to get the latest date per ticker.
        cur.execute(
            """
            SELECT t.ticker, t.dt
            FROM (
              SELECT DISTINCT ON (ticker) ticker, dt
              FROM prices_1d
              WHERE ticker = ANY(%s)
              ORDER BY ticker, dt DESC
            ) AS t;
            """,
            (tickers,),
        )
        rows = cur.fetchall()
    return {str(t): dt for t, dt in rows}


def get_global_latest_1d_dt(conn: psycopg.Connection, tickers: list[str]) -> date | None:
    if not tickers:
        return None
    with conn.cursor() as cur:
        cur.execute("SELECT MAX(dt) FROM prices_1d WHERE ticker = ANY(%s);", (tickers,))
        row = cur.fetchone()
    return None if not row or row[0] is None else row[0]


def cleanup_1m(conn: psycopg.Connection, keep_hours: int) -> None:
    cutoff = datetime.now(timezone.utc) - timedelta(hours=keep_hours)
    with conn.cursor() as cur:
        cur.execute("DELETE FROM prices_1m WHERE ts < %s;", (cutoff,))


def truncate_1m(conn: psycopg.Connection) -> None:
    with conn.cursor() as cur:
        cur.execute("TRUNCATE TABLE prices_1m;")


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
    intraday_truncate_before_load = bool(ucfg.get("intraday_truncate_before_load", True))
    daily_start_mode = str(ucfg.get("daily_start_mode", "from_db")).lower()
    daily_overlap_days = max(0, int(ucfg.get("daily_overlap_days", 3)))
    daily_bootstrap_lookback_days = max(1, int(ucfg.get("daily_bootstrap_lookback_days", lookback)))
    valid_start_modes = {"from_db", "from_db_global", "fixed_lookback"}
    if daily_start_mode not in valid_start_modes:
        LOGGER.warning("invalid daily_start_mode=%s, defaulting to from_db", daily_start_mode)
        daily_start_mode = "from_db"

    today_utc = datetime.now(timezone.utc).date()
    fallback_start_fixed = today_utc - timedelta(days=lookback)
    fallback_start_bootstrap = today_utc - timedelta(days=daily_bootstrap_lookback_days)
    total_tickers = len(tickers)
    run_started = monotonic()
    success_count = 0
    failed_tickers = []

    LOGGER.info(
        "run start tickers=%s daily_start_mode=%s overlap_days=%s intraday_enabled=%s intraday_period=%s",
        total_tickers,
        daily_start_mode,
        daily_overlap_days,
        intraday_enabled,
        intraday_period,
    )

    with get_conn(cfg) as conn:
        ensure_tables(conn)
        latest_1d_map: dict[str, date] = {}
        latest_1d_global: date | None = None
        if daily_start_mode == "from_db":
            latest_1d_map = get_latest_1d_dt_map(conn, tickers)
        elif daily_start_mode == "from_db_global":
            latest_1d_global = get_global_latest_1d_dt(conn, tickers)

        if intraday_enabled and intraday_truncate_before_load:
            try:
                truncate_1m(conn)
                conn.commit()
                LOGGER.info("truncated prices_1m before intraday load")
            except Exception:
                conn.rollback()
                LOGGER.exception("failed truncating prices_1m before intraday load")
                return 1

        for idx, t in enumerate(tickers, start=1):
            ticker_started = monotonic()
            try:
                if daily_start_mode == "from_db":
                    latest_dt = latest_1d_map.get(t)
                    if latest_dt is None:
                        start_day = fallback_start_bootstrap
                    else:
                        start_day = latest_dt - timedelta(days=daily_overlap_days)
                elif daily_start_mode == "from_db_global":
                    if latest_1d_global is None:
                        start_day = fallback_start_bootstrap
                    else:
                        start_day = latest_1d_global - timedelta(days=daily_overlap_days)
                else:
                    start_day = fallback_start_fixed

                start_dt = start_day.isoformat()
                pct = (idx / total_tickers) * 100.0 if total_tickers > 0 else 100.0
                LOGGER.info("[%s/%s %.1f%%] ticker=%s start daily_start=%s", idx, total_tickers, pct, t, start_dt)
                LOGGER.info("[%s/%s] ticker=%s downloading 1d", idx, total_tickers, t)
                df1d = yf.download(t, start=start_dt, interval="1d", auto_adjust=False, progress=False)
                df1d = normalize_ohlcv(df1d, t)
                upsert_1d(conn, t, df1d)

                intraday_rows = 0
                if intraday_enabled:
                    LOGGER.info(
                        "[%s/%s] ticker=%s downloading 1m period=%s",
                        idx,
                        total_tickers,
                        t,
                        intraday_period,
                    )
                    df1m = yf.download(t, period=intraday_period, interval="1m", auto_adjust=False, progress=False)
                    df1m = normalize_ohlcv(df1m, t)
                    intraday_rows = 0 if df1m is None else len(df1m)
                    upsert_1m(conn, t, df1m)

                conn.commit()
                success_count += 1
                ticker_elapsed = monotonic() - ticker_started
                run_elapsed = monotonic() - run_started
                avg_per_ticker = run_elapsed / idx if idx > 0 else 0.0
                eta_seconds = avg_per_ticker * (total_tickers - idx)
                LOGGER.info(
                    "[%s/%s %.1f%%] ticker=%s done daily_start=%s daily_rows=%s intraday_rows=%s "
                    "ticker_elapsed=%.1fs run_elapsed=%.1fs eta=%.1fs",
                    idx,
                    total_tickers,
                    pct,
                    t,
                    start_dt,
                    0 if df1d is None else len(df1d),
                    intraday_rows,
                    ticker_elapsed,
                    run_elapsed,
                    eta_seconds,
                )
            except Exception:
                conn.rollback()
                failed_tickers.append(t)
                ticker_elapsed = monotonic() - ticker_started
                run_elapsed = monotonic() - run_started
                pct = (idx / total_tickers) * 100.0 if total_tickers > 0 else 100.0
                LOGGER.error(
                    "[%s/%s %.1f%%] ticker=%s failed ticker_elapsed=%.1fs run_elapsed=%.1fs",
                    idx,
                    total_tickers,
                    pct,
                    t,
                    ticker_elapsed,
                    run_elapsed,
                )
                LOGGER.exception("failed updating ticker=%s", t)

        if intraday_enabled and not intraday_truncate_before_load:
            try:
                cleanup_1m(conn, keep_1m)
                conn.commit()
            except Exception:
                conn.rollback()
                LOGGER.exception("failed intraday cleanup")
                return 1

    LOGGER.info(
        "run complete total=%s success=%s failed=%s total_elapsed=%.1fs",
        len(tickers),
        success_count,
        len(failed_tickers),
        monotonic() - run_started,
    )
    if failed_tickers:
        LOGGER.warning("failed tickers: %s", ",".join(failed_tickers))

    if success_count == 0:
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
