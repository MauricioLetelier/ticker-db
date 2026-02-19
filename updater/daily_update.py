import os
import logging
import sys
from datetime import date, datetime, timedelta, timezone
from time import monotonic, sleep

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
    connect_timeout = int(os.getenv("DB_CONNECT_TIMEOUT_SECS", "15"))
    LOGGER.info(
        "connecting to db host=%s port=%s db=%s user=%s timeout=%ss",
        db["host"],
        db["port"],
        db["name"],
        db["user"],
        connect_timeout,
    )
    conn_str = (
        f"host={db['host']} port={db['port']} dbname={db['name']} "
        f"user={db['user']} password={db_pass} connect_timeout={connect_timeout}"
    )
    conn = psycopg.connect(conn_str)
    LOGGER.info("db connection established")
    return conn


def ensure_tables(conn: psycopg.Connection, ddl_lock_timeout_s: int = 10) -> None:
    with conn.cursor() as cur:
        safe_timeout = max(1, int(ddl_lock_timeout_s))
        cur.execute(f"SET LOCAL lock_timeout = '{safe_timeout}s';")
        cur.execute(f"SET LOCAL statement_timeout = '{max(5, safe_timeout * 2)}s';")
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


def required_tables_exist(conn: psycopg.Connection) -> bool:
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT
              to_regclass('public.prices_1d') IS NOT NULL AS has_1d,
              to_regclass('public.prices_1m') IS NOT NULL AS has_1m;
            """
        )
        row = cur.fetchone()
    return bool(row and row[0] and row[1])


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


def configure_db_session_timeouts(
    conn: psycopg.Connection,
    *,
    lock_timeout_s: int,
    statement_timeout_s: int,
) -> None:
    safe_lock = max(1, int(lock_timeout_s))
    safe_stmt = max(5, int(statement_timeout_s))
    with conn.cursor() as cur:
        cur.execute("SELECT set_config('lock_timeout', %s, false);", (f"{safe_lock}s",))
        cur.execute("SELECT set_config('statement_timeout', %s, false);", (f"{safe_stmt}s",))


def download_ohlcv(
    ticker: str,
    *,
    interval: str,
    start: str | None = None,
    period: str | None = None,
    timeout_s: int = 25,
    retries: int = 2,
    retry_backoff_s: float = 2.0,
) -> pd.DataFrame:
    if start is None and period is None:
        raise ValueError("Either start or period must be provided.")

    kwargs = {
        "tickers": ticker,
        "interval": interval,
        "auto_adjust": False,
        "progress": False,
        "threads": False,
        "timeout": max(5, int(timeout_s)),
    }
    if start is not None:
        kwargs["start"] = start
    if period is not None:
        kwargs["period"] = period

    max_attempts = max(1, int(retries) + 1)
    last_exc: Exception | None = None

    for attempt in range(1, max_attempts + 1):
        started = monotonic()
        try:
            df = yf.download(**kwargs)
            elapsed = monotonic() - started
            rows = 0 if df is None else len(df)
            LOGGER.info(
                "yf.download ticker=%s interval=%s attempt=%s/%s rows=%s elapsed=%.1fs",
                ticker,
                interval,
                attempt,
                max_attempts,
                rows,
                elapsed,
            )
            return df
        except Exception as exc:
            elapsed = monotonic() - started
            last_exc = exc
            LOGGER.warning(
                "yf.download failed ticker=%s interval=%s attempt=%s/%s elapsed=%.1fs err=%s",
                ticker,
                interval,
                attempt,
                max_attempts,
                elapsed,
                repr(exc),
            )
            if attempt < max_attempts:
                wait_s = max(0.5, float(retry_backoff_s)) * attempt
                LOGGER.info(
                    "retrying ticker=%s interval=%s after %.1fs (%s retries left)",
                    ticker,
                    interval,
                    wait_s,
                    max_attempts - attempt,
                )
                sleep(wait_s)

    raise RuntimeError(
        f"yf.download exhausted retries ticker={ticker} interval={interval} attempts={max_attempts}"
    ) from last_exc


def cleanup_1m(conn: psycopg.Connection, keep_hours: int) -> None:
    cutoff = datetime.now(timezone.utc) - timedelta(hours=keep_hours)
    with conn.cursor() as cur:
        cur.execute("DELETE FROM prices_1m WHERE ts < %s;", (cutoff,))


def truncate_1m(conn: psycopg.Connection, lock_timeout_s: int = 15) -> None:
    with conn.cursor() as cur:
        safe_lock_timeout_s = max(1, int(lock_timeout_s))
        cur.execute(f"SET LOCAL lock_timeout = '{safe_lock_timeout_s}s';")
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
    intraday_truncate_lock_timeout_s = max(1, int(ucfg.get("intraday_truncate_lock_timeout_s", 15)))
    schema_ddl_lock_timeout_s = max(1, int(ucfg.get("schema_ddl_lock_timeout_s", 10)))
    db_lock_timeout_s = max(1, int(ucfg.get("db_lock_timeout_s", 15)))
    db_statement_timeout_s = max(5, int(ucfg.get("db_statement_timeout_s", 120)))
    yfinance_timeout_s = max(5, int(ucfg.get("yfinance_timeout_s", 25)))
    yfinance_retries = max(0, int(ucfg.get("yfinance_retries", 2)))
    yfinance_retry_backoff_s = max(0.5, float(ucfg.get("yfinance_retry_backoff_s", 2.0)))
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
        "run start tickers=%s daily_start_mode=%s overlap_days=%s intraday_enabled=%s intraday_period=%s "
        "yf_timeout_s=%s yf_retries=%s",
        total_tickers,
        daily_start_mode,
        daily_overlap_days,
        intraday_enabled,
        intraday_period,
        yfinance_timeout_s,
        yfinance_retries,
    )

    LOGGER.info("opening db session")
    with get_conn(cfg) as conn:
        configure_db_session_timeouts(
            conn,
            lock_timeout_s=db_lock_timeout_s,
            statement_timeout_s=db_statement_timeout_s,
        )
        LOGGER.info(
            "db session timeouts configured lock_timeout=%ss statement_timeout=%ss",
            db_lock_timeout_s,
            db_statement_timeout_s,
        )
        LOGGER.info("checking required tables")
        if required_tables_exist(conn):
            LOGGER.info("required tables exist; skipping DDL ensure")
        else:
            LOGGER.info("required tables missing; ensuring tables/indexes (ddl_lock_timeout=%ss)", schema_ddl_lock_timeout_s)
            ensure_tables(conn, ddl_lock_timeout_s=schema_ddl_lock_timeout_s)
            conn.commit()
            LOGGER.info("table/index ensure complete")
        latest_1d_map: dict[str, date] = {}
        latest_1d_global: date | None = None
        if daily_start_mode == "from_db":
            LOGGER.info("loading per-ticker latest 1d dates in one query")
            latest_1d_map = get_latest_1d_dt_map(conn, tickers)
            LOGGER.info("loaded latest 1d dates for %s/%s tickers", len(latest_1d_map), total_tickers)
        elif daily_start_mode == "from_db_global":
            LOGGER.info("loading global latest 1d date for selected ticker set")
            latest_1d_global = get_global_latest_1d_dt(conn, tickers)
            LOGGER.info("loaded global latest 1d date: %s", latest_1d_global)

        if intraday_enabled and intraday_truncate_before_load:
            try:
                LOGGER.info(
                    "truncating prices_1m before intraday load (lock_timeout=%ss)",
                    intraday_truncate_lock_timeout_s,
                )
                truncate_1m(conn, lock_timeout_s=intraday_truncate_lock_timeout_s)
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
                df1d = download_ohlcv(
                    t,
                    interval="1d",
                    start=start_dt,
                    timeout_s=yfinance_timeout_s,
                    retries=yfinance_retries,
                    retry_backoff_s=yfinance_retry_backoff_s,
                )
                df1d = normalize_ohlcv(df1d, t)
                LOGGER.info(
                    "[%s/%s] ticker=%s upserting 1d rows=%s",
                    idx,
                    total_tickers,
                    t,
                    0 if df1d is None else len(df1d),
                )
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
                    df1m = download_ohlcv(
                        t,
                        interval="1m",
                        period=intraday_period,
                        timeout_s=yfinance_timeout_s,
                        retries=yfinance_retries,
                        retry_backoff_s=yfinance_retry_backoff_s,
                    )
                    df1m = normalize_ohlcv(df1m, t)
                    intraday_rows = 0 if df1m is None else len(df1m)
                    LOGGER.info(
                        "[%s/%s] ticker=%s upserting 1m rows=%s",
                        idx,
                        total_tickers,
                        t,
                        intraday_rows,
                    )
                    upsert_1m(conn, t, df1m)

                LOGGER.info("[%s/%s] ticker=%s committing transaction", idx, total_tickers, t)
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
