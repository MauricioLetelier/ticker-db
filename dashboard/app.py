# dashboard/app.py
# Sector/Subsector ETF Dashboard + Custom Basket Builder (writes classifications)
# Copy/paste this whole file.
#
# Requirements:
#   streamlit, pandas, psycopg[binary], plotly
# Env vars:
#   DB_HOST, DB_PORT, DB_NAME, DB_USER, DB_PASS
#
# Notes:
# - This file assumes you have these tables/constraints:
#     sector(sector_id pk, sector_name unique)
#     subsector(subsector_id pk, sector_id fk, subsector_name, unique(sector_id, subsector_name))
#     instrument(ticker pk or unique)
#     instrument_classification(ticker fk, subsector_id fk, is_primary boolean, unique(ticker, subsector_id))
#     prices_1d(ticker, dt, close)
# - For market-cap weighting, it tries:
#     1) instrument_market_cap(ticker, dt, market_cap_usd)
#     2) fundamentals_quarterly(ticker, report_dt, shares_outstanding) * latest close
#   If neither exists, it falls back to equal weights and shows a warning.

import os
from datetime import date
from itertools import product
from typing import Sequence, Literal, Tuple, Callable

import pandas as pd
import psycopg
import streamlit as st
import plotly.express as px

# -----------------------------
# Page config
# -----------------------------
st.set_page_config(page_title="Sector/Subsector ETF Dashboard", layout="wide")

DB_HOST = os.getenv("DB_HOST", "postgres")
DB_PORT = int(os.getenv("DB_PORT", "5432"))
DB_NAME = os.getenv("DB_NAME", "appdb")
DB_USER = os.getenv("DB_USER", "appuser")
DB_PASS = os.getenv("DB_PASS", "")

WeightMethod = Literal["equal", "market_cap"]
CHART_COLORS = [
    "#2563eb",
    "#0f766e",
    "#b45309",
    "#dc2626",
    "#0ea5a4",
    "#475569",
    "#16a34a",
    "#1d4ed8",
]


def inject_custom_css() -> None:
    st.markdown(
        """
        <style>
          @import url('https://fonts.googleapis.com/css2?family=Manrope:wght@400;500;600;700;800&family=IBM+Plex+Mono:wght@500&display=swap');

          :root {
            --bg-main: #f6f8fb;
            --bg-gradient-a: #f8faff;
            --bg-gradient-b: #f5f9f8;
            --card-bg: rgba(255, 255, 255, 0.94);
            --card-border: rgba(15, 23, 42, 0.09);
            --text-main: #0f172a;
            --text-muted: #475569;
            --accent: #2563eb;
            --accent-soft: rgba(37, 99, 235, 0.10);
            --shadow-soft: 0 1px 2px rgba(15, 23, 42, 0.06), 0 8px 24px rgba(15, 23, 42, 0.05);
          }

          .stApp {
            background:
              radial-gradient(1100px 460px at 8% -15%, var(--bg-gradient-a), transparent 62%),
              radial-gradient(900px 420px at 102% -8%, var(--bg-gradient-b), transparent 62%),
              var(--bg-main);
            color: var(--text-main);
            font-family: "Manrope", "Segoe UI", sans-serif;
          }

          [data-testid="stSidebar"] {
            background: rgba(255, 255, 255, 0.94);
            border-right: 1px solid var(--card-border);
          }

          h1, h2, h3 {
            font-family: "Manrope", "Segoe UI", sans-serif;
            letter-spacing: -0.02em;
            color: #0f172a;
            font-weight: 700;
          }

          .hero {
            padding: 1rem 1.1rem;
            border: 1px solid var(--card-border);
            border-radius: 12px;
            background: var(--card-bg);
            box-shadow: var(--shadow-soft);
            margin: 0.15rem 0 1rem 0;
            position: relative;
          }

          .hero::before {
            content: '';
            position: absolute;
            left: 0;
            top: 0;
            bottom: 0;
            width: 4px;
            border-radius: 12px 0 0 12px;
            background: linear-gradient(180deg, #2563eb 0%, #0f766e 100%);
          }

          .hero-title {
            margin: 0 0 0 0.35rem;
            font-family: "Manrope", "Segoe UI", sans-serif;
            font-size: 1.35rem;
            letter-spacing: -0.02em;
            font-weight: 800;
            color: #0f172a;
          }

          .hero-subtitle {
            margin: 0.25rem 0 0 0.35rem;
            color: var(--text-muted);
            font-size: 0.92rem;
          }

          [data-testid="stMetric"] {
            border: 1px solid var(--card-border);
            border-radius: 12px;
            padding: 0.55rem 0.7rem 0.5rem 0.7rem;
            background: var(--card-bg);
            box-shadow: 0 1px 2px rgba(15, 23, 42, 0.05);
          }

          [data-testid="stMetricLabel"] {
            color: #64748b;
            font-weight: 600;
            letter-spacing: 0.01em;
          }

          [data-testid="stMetricValue"] {
            color: #0f172a;
            font-family: "IBM Plex Mono", ui-monospace, monospace;
            font-size: 1.35rem;
            font-variant-numeric: tabular-nums;
          }

          [data-testid="stDataFrame"] {
            border: 1px solid var(--card-border);
            border-radius: 12px;
            overflow: hidden;
            box-shadow: 0 1px 2px rgba(15, 23, 42, 0.04);
          }

          [data-testid="stForm"] {
            border: 1px solid var(--card-border);
            border-radius: 12px;
            background: var(--card-bg);
            padding: 0.8rem 0.85rem 0.6rem 0.85rem;
            box-shadow: var(--shadow-soft);
          }

          [data-baseweb="select"] > div,
          .stTextInput > div > div > input,
          .stDateInput > div > div input,
          .stTextArea > div > div > textarea {
            border-radius: 10px;
            border-color: rgba(15, 23, 42, 0.14);
            background: #ffffff;
          }

          .stTabs [role="tab"] {
            border-radius: 8px;
          }

          .stButton > button,
          div[data-testid="stFormSubmitButton"] button {
            background: #2563eb;
            color: white;
            border: 1px solid #1d4ed8;
            border-radius: 10px;
            font-weight: 600;
            transition: transform .14s ease, box-shadow .14s ease;
            box-shadow: 0 1px 2px rgba(15, 23, 42, 0.08), 0 6px 14px rgba(37, 99, 235, 0.18);
          }

          .stButton > button:hover,
          div[data-testid="stFormSubmitButton"] button:hover {
            background: #1d4ed8;
            box-shadow: 0 8px 18px rgba(37, 99, 235, 0.2);
          }
        </style>
        """,
        unsafe_allow_html=True,
    )


def style_figure(fig, title: str | None = None):
    fig.update_layout(
        template="plotly_white",
        font={"family": "Manrope, Segoe UI, sans-serif", "size": 12, "color": "#0f172a"},
        colorway=CHART_COLORS,
        paper_bgcolor="rgba(0,0,0,0)",
        plot_bgcolor="#ffffff",
        hovermode="x unified",
        margin={"l": 22, "r": 14, "t": 62, "b": 24},
        legend={"orientation": "h", "yanchor": "bottom", "y": 1.02, "xanchor": "left", "x": 0},
    )
    if title:
        fig.update_layout(title={"text": title, "x": 0.01, "font": {"size": 16}})
    fig.update_xaxes(showgrid=False, zeroline=False)
    fig.update_yaxes(showgrid=True, gridcolor="rgba(15, 23, 42, 0.08)", zeroline=False)
    return fig


# -----------------------------
# DB helpers
# -----------------------------
@st.cache_resource
def get_conn():
    # Note: cached connection is fine for light usage.
    # If you hit "connection closed" errors, we can switch to a pool.
    return psycopg.connect(
        host=DB_HOST, port=DB_PORT, dbname=DB_NAME, user=DB_USER, password=DB_PASS
    )


def qdf(sql: str, params=None) -> pd.DataFrame:
    with get_conn().cursor() as cur:
        cur.execute(sql, params or ())
        rows = cur.fetchall()
        cols = [d.name for d in cur.description]
    return pd.DataFrame(rows, columns=cols)


# -----------------------------
# Upsert + classification utilities
# -----------------------------
def ensure_sector_and_subsector(conn, sector_name: str, subsector_name: str) -> int:
    """
    Ensures sector + subsector exist. Returns subsector_id.

    Requires unique constraints:
      - sector(sector_name)
      - subsector(sector_id, subsector_name)
    """
    sector_name = sector_name.strip()
    subsector_name = subsector_name.strip()
    if not sector_name or not subsector_name:
        raise ValueError("sector_name and subsector_name must be non-empty")

    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO sector (sector_name)
            VALUES (%s)
            ON CONFLICT (sector_name) DO UPDATE
              SET sector_name = EXCLUDED.sector_name
            RETURNING sector_id
            """,
            (sector_name,),
        )
        sector_id = cur.fetchone()[0]

        cur.execute(
            """
            INSERT INTO subsector (sector_id, subsector_name)
            VALUES (%s, %s)
            ON CONFLICT (sector_id, subsector_name) DO UPDATE
              SET subsector_name = EXCLUDED.subsector_name
            RETURNING subsector_id
            """,
            (sector_id, subsector_name),
        )
        subsector_id = cur.fetchone()[0]

    return int(subsector_id)


def upsert_tickers_and_classification(
    conn,
    tickers: Sequence[str],
    subsector_id: int,
    *,
    is_primary: bool = False,
) -> None:
    """
    Ensures instrument rows exist and creates/updates instrument_classification links.

    Requires unique constraints:
      - instrument(ticker)
      - instrument_classification(ticker, subsector_id)
    """
    tickers = [t.strip().upper() for t in tickers if t and t.strip()]
    if not tickers:
        return

    with conn.cursor() as cur:
        # ensure instruments exist
        cur.execute(
            """
            INSERT INTO instrument (ticker)
            SELECT UNNEST(%s::text[])
            ON CONFLICT (ticker) DO NOTHING
            """,
            (tickers,),
        )

        # attach classification
        cur.execute(
            """
            INSERT INTO instrument_classification (ticker, subsector_id, is_primary)
            SELECT UNNEST(%s::text[]), %s, %s
            ON CONFLICT (ticker, subsector_id) DO UPDATE
              SET is_primary = EXCLUDED.is_primary
            """,
            (tickers, subsector_id, is_primary),
        )


# -----------------------------
# Market cap + weights
# -----------------------------
def fetch_market_caps(conn, tickers: Sequence[str]) -> pd.DataFrame:
    """
    Returns DataFrame: ticker, market_cap (float)

    Tries:
      1) instrument_market_cap(ticker, dt, market_cap_usd) latest per ticker
      2) fundamentals_quarterly(ticker, report_dt, shares_outstanding) * latest close
    Falls back to empty -> caller handles equal weight.
    """
    tickers = [t.strip().upper() for t in tickers if t and t.strip()]
    if not tickers:
        return pd.DataFrame(columns=["ticker", "market_cap"])

    with conn.cursor() as cur:
        # 1) instrument_market_cap
        try:
            cur.execute(
                """
                WITH mc AS (
                  SELECT
                    imc.ticker,
                    imc.market_cap_usd::float AS market_cap,
                    ROW_NUMBER() OVER (PARTITION BY imc.ticker ORDER BY imc.dt DESC) AS rn
                  FROM instrument_market_cap imc
                  WHERE imc.ticker = ANY(%s)
                )
                SELECT ticker, market_cap
                FROM mc
                WHERE rn = 1
                """,
                (tickers,),
            )
            rows = cur.fetchall()
            if rows:
                return pd.DataFrame(rows, columns=["ticker", "market_cap"])
        except Exception:
            conn.rollback()

        # 2) compute from shares_outstanding * latest close
        try:
            cur.execute(
                """
                WITH last_px AS (
                  SELECT DISTINCT ON (p.ticker)
                    p.ticker, p.close::float AS close
                  FROM prices_1d p
                  WHERE p.ticker = ANY(%s)
                  ORDER BY p.ticker, p.dt DESC
                ),
                last_sh AS (
                  SELECT DISTINCT ON (f.ticker)
                    f.ticker, f.shares_outstanding::float AS shares_outstanding
                  FROM fundamentals_quarterly f
                  WHERE f.ticker = ANY(%s)
                    AND f.shares_outstanding IS NOT NULL
                  ORDER BY f.ticker, f.report_dt DESC
                )
                SELECT
                  px.ticker,
                  (px.close * sh.shares_outstanding) AS market_cap
                FROM last_px px
                JOIN last_sh sh USING (ticker)
                """,
                (tickers, tickers),
            )
            rows = cur.fetchall()
            if rows:
                return pd.DataFrame(rows, columns=["ticker", "market_cap"])
        except Exception:
            conn.rollback()

    return pd.DataFrame(columns=["ticker", "market_cap"])


def compute_weights(conn, tickers: Sequence[str], method: WeightMethod) -> Tuple[pd.DataFrame, str | None]:
    """
    Returns (weights_df, warning_message_or_none)
    weights_df columns: ticker, w
    """
    tickers = [t.strip().upper() for t in tickers if t and t.strip()]
    n = len(tickers)
    if n == 0:
        return pd.DataFrame(columns=["ticker", "w"]), None

    if method == "equal":
        return pd.DataFrame({"ticker": tickers, "w": [1.0 / n] * n}), None

    mc = fetch_market_caps(conn, tickers)
    if mc.empty or mc["market_cap"].isna().all():
        return pd.DataFrame({"ticker": tickers, "w": [1.0 / n] * n}), (
            "Market-cap data not found (instrument_market_cap or fundamentals_quarterly). "
            "Falling back to equal weights."
        )

    mc = mc.dropna()
    if mc.empty:
        return pd.DataFrame({"ticker": tickers, "w": [1.0 / n] * n}), (
            "Market-cap data missing for all tickers. Falling back to equal weights."
        )

    total = float(mc["market_cap"].sum())
    if total <= 0:
        return pd.DataFrame({"ticker": tickers, "w": [1.0 / n] * n}), (
            "Market-cap sum <= 0. Falling back to equal weights."
        )

    mc["w"] = mc["market_cap"] / total

    # Ensure all tickers included; missing caps get 0 then renormalize
    out = pd.DataFrame({"ticker": tickers}).merge(mc[["ticker", "w"]], on="ticker", how="left").fillna(0.0)
    s = float(out["w"].sum())
    if s <= 0:
        out["w"] = 1.0 / n
        return out, "Market-cap weights collapsed to 0. Falling back to equal weights."

    out["w"] = out["w"] / s
    return out, None


def fetch_weighted_basket_series(conn, tickers: Sequence[str], start, end, method: WeightMethod):
    """
    Returns (basket_df, weights_df, warning_or_none)
    basket_df columns: dt, basket, basket_norm
    """
    weights_df, warn = compute_weights(conn, tickers, method)
    if weights_df.empty:
        return pd.DataFrame(columns=["dt", "basket", "basket_norm"]), weights_df, warn

    tickers_arr = weights_df["ticker"].tolist()
    weights_arr = weights_df["w"].astype(float).tolist()

    sql = """
    WITH w AS (
      SELECT
        UNNEST(%s::text[]) AS ticker,
        UNNEST(%s::float8[]) AS w
    ),
    px AS (
      SELECT p.dt, p.ticker, p.close::float AS close
      FROM prices_1d p
      WHERE p.ticker = ANY(%s)
        AND p.dt >= %s
        AND p.dt <= %s
    ),
    agg AS (
      SELECT
        px.dt,
        SUM(px.close * w.w) AS basket
      FROM px
      JOIN w USING (ticker)
      GROUP BY px.dt
      ORDER BY px.dt
    ),
    base AS (
      SELECT
        dt,
        basket,
        FIRST_VALUE(basket) OVER (ORDER BY dt) AS base_basket
      FROM agg
    )
    SELECT
      dt,
      basket,
      CASE WHEN base_basket IS NULL OR base_basket = 0 THEN NULL ELSE basket / base_basket END AS basket_norm
    FROM base
    ORDER BY dt;
    """
    with conn.cursor() as cur:
        cur.execute(sql, (tickers_arr, weights_arr, tickers_arr, start, end))
        rows = cur.fetchall()
        cols = [d.name for d in cur.description]
    basket_df = pd.DataFrame(rows, columns=cols)
    if not basket_df.empty:
        basket_df["dt"] = pd.to_datetime(basket_df["dt"])

    return basket_df, weights_df, warn


def create_or_update_subsector_basket(
    conn,
    *,
    sector_name: str,
    subsector_name: str,
    tickers: Sequence[str],
    weight_method: WeightMethod,
    start,
    end,
    is_primary: bool = False,
):
    """
    1) Ensure sector/subsector exist
    2) Attach tickers to that subsector
    3) Return weighted basket series + weights used
    """
    subsector_id = ensure_sector_and_subsector(conn, sector_name, subsector_name)
    upsert_tickers_and_classification(conn, tickers, subsector_id, is_primary=is_primary)
    conn.commit()

    basket_df, weights_df, warn = fetch_weighted_basket_series(conn, tickers, start, end, weight_method)
    return subsector_id, basket_df, weights_df, warn


# -----------------------------
# UI helpers
# -----------------------------
def resolve_tickers(sector_choice: str, subsector_choice: str) -> list[str]:
    if sector_choice == "All" and subsector_choice == "All":
        return qdf(
            """
            SELECT DISTINCT i.ticker
            FROM instrument i
            ORDER BY i.ticker;
            """
        )["ticker"].tolist()
    if sector_choice != "All" and subsector_choice == "All":
        return qdf(
            """
            SELECT DISTINCT i.ticker
            FROM instrument i
            JOIN instrument_classification ic ON ic.ticker = i.ticker AND ic.is_primary
            JOIN subsector sc ON sc.subsector_id = ic.subsector_id
            JOIN sector se ON se.sector_id = sc.sector_id
            WHERE se.sector_name = %s
            ORDER BY i.ticker;
            """,
            (sector_choice,),
        )["ticker"].tolist()
    if sector_choice == "All" and subsector_choice != "All":
        return qdf(
            """
            SELECT DISTINCT i.ticker
            FROM instrument i
            JOIN instrument_classification ic ON ic.ticker = i.ticker AND ic.is_primary
            JOIN subsector sc ON sc.subsector_id = ic.subsector_id
            WHERE sc.subsector_name = %s
            ORDER BY i.ticker;
            """,
            (subsector_choice,),
        )["ticker"].tolist()
    return qdf(
        """
        SELECT DISTINCT i.ticker
        FROM instrument i
        JOIN instrument_classification ic ON ic.ticker = i.ticker AND ic.is_primary
        JOIN subsector sc ON sc.subsector_id = ic.subsector_id
        JOIN sector se ON se.sector_id = sc.sector_id
        WHERE se.sector_name = %s AND sc.subsector_name = %s
        ORDER BY i.ticker;
        """,
        (sector_choice, subsector_choice),
    )["ticker"].tolist()


def render_classification_filters(key_prefix: str) -> tuple[str, str, list[str]]:
    sectors = qdf("SELECT sector_name FROM sector ORDER BY sector_name;")["sector_name"].tolist()
    sector_choice = st.selectbox("Sector", ["All"] + sectors, index=0, key=f"{key_prefix}_sector")

    if sector_choice == "All":
        subsectors = qdf(
            """
            SELECT sc.subsector_name
            FROM subsector sc
            JOIN sector se ON se.sector_id = sc.sector_id
            ORDER BY se.sector_name, sc.subsector_name;
            """
        )["subsector_name"].tolist()
    else:
        subsectors = qdf(
            """
            SELECT sc.subsector_name
            FROM subsector sc
            JOIN sector se ON se.sector_id = sc.sector_id
            WHERE se.sector_name = %s
            ORDER BY sc.subsector_name;
            """,
            (sector_choice,),
        )["subsector_name"].tolist()

    subsector_choice = st.selectbox(
        "Subsector",
        ["All"] + subsectors,
        index=0,
        key=f"{key_prefix}_subsector",
    )
    tickers = resolve_tickers(sector_choice, subsector_choice)
    return sector_choice, subsector_choice, tickers


def fetch_normalized_series(tickers: Sequence[str], start_dt, end_dt) -> pd.DataFrame:
    sql = """
    WITH base AS (
      SELECT
        p.ticker,
        p.dt,
        p.close,
        FIRST_VALUE(p.close) OVER (PARTITION BY p.ticker ORDER BY p.dt) AS base_close
      FROM prices_1d p
      WHERE p.ticker = ANY(%s)
        AND p.dt >= %s
        AND p.dt <= %s
    ),
    norm AS (
      SELECT
        ticker,
        dt,
        close,
        CASE WHEN base_close IS NULL OR base_close = 0 THEN NULL ELSE close / base_close END AS norm_close
      FROM base
    )
    SELECT ticker, dt, close, norm_close
    FROM norm
    ORDER BY dt, ticker;
    """
    out = qdf(sql, (list(tickers), start_dt, end_dt))
    if not out.empty:
        out["dt"] = pd.to_datetime(out["dt"])
    return out


def fetch_top_performers(tickers: Sequence[str], lookback_days: int, top_n: int) -> pd.DataFrame:
    sql = """
    WITH latest AS (
      SELECT DISTINCT ON (p.ticker)
        p.ticker,
        p.dt AS latest_dt,
        p.close::float AS latest_close
      FROM prices_1d p
      WHERE p.ticker = ANY(%s)
        AND p.close IS NOT NULL
      ORDER BY p.ticker, p.dt DESC
    ),
    base AS (
      SELECT
        l.ticker,
        l.latest_dt,
        l.latest_close,
        p0.dt AS start_dt,
        p0.close::float AS start_close
      FROM latest l
      LEFT JOIN LATERAL (
        SELECT p.dt, p.close
        FROM prices_1d p
        WHERE p.ticker = l.ticker
          AND p.close IS NOT NULL
          AND p.dt <= (l.latest_dt - %s::int)
        ORDER BY p.dt DESC
        LIMIT 1
      ) p0 ON TRUE
    )
    SELECT
      ticker,
      start_dt,
      latest_dt,
      start_close,
      latest_close,
      CASE
        WHEN start_close IS NULL OR start_close = 0 THEN NULL
        ELSE ((latest_close / start_close) - 1.0) * 100.0
      END AS return_pct
    FROM base
    WHERE start_close IS NOT NULL
    ORDER BY return_pct DESC NULLS LAST, ticker
    LIMIT %s;
    """
    return qdf(sql, (list(tickers), lookback_days, top_n))


def render_top_performer_block(title: str, lookback_days: int, tickers: Sequence[str], top_n: int) -> None:
    rank_df = fetch_top_performers(tickers, lookback_days=lookback_days, top_n=top_n)
    st.subheader(title)
    if rank_df.empty:
        st.warning(
            f"No ranking data found for last {lookback_days} days "
            "(missing backfill for some symbols)."
        )
        return

    rank_df["start_dt"] = pd.to_datetime(rank_df["start_dt"])
    rank_df["latest_dt"] = pd.to_datetime(rank_df["latest_dt"])
    leader = rank_df.iloc[0]
    m1, m2, m3 = st.columns(3)
    with m1:
        st.metric("Leader", leader["ticker"], f"{leader['return_pct']:.2f}%")
    with m2:
        st.metric("Avg Return", f"{rank_df['return_pct'].mean():.2f}%")
    with m3:
        st.metric("Constituents", f"{len(rank_df)}")
    st.dataframe(rank_df, use_container_width=True)

    bar_df = rank_df.sort_values("return_pct", ascending=True).copy()
    bar_fig = px.bar(
        bar_df,
        x="return_pct",
        y="ticker",
        orientation="h",
        title=f"Ranked returns ({lookback_days}d)",
        labels={"return_pct": "Return %", "ticker": ""},
        color="return_pct",
        color_continuous_scale=["#86c5da", "#1b6ca8", "#f18f01"],
    )
    bar_fig.update_layout(coloraxis_showscale=False)
    st.plotly_chart(style_figure(bar_fig), use_container_width=True)

    chart_start = rank_df["start_dt"].min().date()
    chart_end = rank_df["latest_dt"].max().date()
    ranked_tickers = rank_df["ticker"].tolist()
    perf_df = fetch_normalized_series(ranked_tickers, chart_start, chart_end)
    if perf_df.empty:
        st.warning("No price series found for ranked tickers.")
        return
    fig = px.line(
        perf_df,
        x="dt",
        y="norm_close",
        color="ticker",
        title=f"Top {top_n} normalized performance ({lookback_days}d ranking window)",
    )
    st.plotly_chart(style_figure(fig), use_container_width=True)


def fetch_close_panel(tickers: Sequence[str], start_dt, end_dt) -> pd.DataFrame:
    sql = """
    SELECT
      p.dt,
      p.ticker,
      p.close::float AS close
    FROM prices_1d p
    WHERE p.ticker = ANY(%s)
      AND p.dt >= %s
      AND p.dt <= %s
      AND p.close IS NOT NULL
    ORDER BY p.dt, p.ticker;
    """
    df = qdf(sql, (list(tickers), start_dt, end_dt))
    if df.empty:
        return pd.DataFrame()
    df["dt"] = pd.to_datetime(df["dt"])
    panel = df.pivot(index="dt", columns="ticker", values="close").sort_index()
    return panel


def run_threshold_simulation(
    price_panel: pd.DataFrame,
    buy_unit_by_ticker: dict[str, float],
    starting_cash: float,
    annual_cash_yield_pct: float,
    annual_borrow_rate_pct: float,
    allow_leverage: bool,
    buy_threshold_pct: float,
    buy_window_days: int,
    sell_threshold_pct: float,
    sell_window_days: int,
    sell_mode: str,
    fee_bps: float,
    allow_reentry: bool,
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    tickers = list(buy_unit_by_ticker.keys())
    fee_buy = 1.0 + (fee_bps / 10000.0)
    fee_sell = 1.0 - (fee_bps / 10000.0)
    annual_cash_yield = float(max(0.0, annual_cash_yield_pct)) / 100.0
    annual_borrow_rate = float(max(0.0, annual_borrow_rate_pct)) / 100.0

    cash_balance = float(max(0.0, starting_cash))
    state = {
        t: {
            "shares": 0.0,
            "last_price": None,
            "buy_count": 0,
            "sell_count": 0,
            "history": [],
            "notional_bought": 0.0,
            "proceeds_sold": 0.0,
        }
        for t in tickers
    }

    trades: list[dict] = []
    equity_curve: list[dict] = []

    def buy_one_unit(ticker: str, dt, price: float, signal_ret: float | None, signal_window: int):
        nonlocal cash_balance
        stt = state[ticker]
        unit_usd = float(buy_unit_by_ticker[ticker])
        if unit_usd <= 0:
            return
        if (not allow_leverage) and cash_balance < unit_usd:
            return

        cash_before = cash_balance
        cash_balance -= unit_usd
        shares_bought = (unit_usd / fee_buy) / price
        stt["shares"] += shares_bought
        stt["buy_count"] += 1
        stt["notional_bought"] += unit_usd

        trades.append(
            {
                "dt": dt,
                "ticker": ticker,
                "action": "BUY",
                "price": price,
                "shares": shares_bought,
                "order_usd": unit_usd,
                "cash_before": cash_before,
                "cash_after": cash_balance,
                "shares_after": stt["shares"],
                "signal_return_pct": signal_ret,
                "signal_window_days": signal_window,
            }
        )

    prev_dt = None
    for dt, row in price_panel.iterrows():
        if prev_dt is not None:
            days_delta = max(0.0, (dt - prev_dt).total_seconds() / 86400.0)
            if days_delta > 0 and cash_balance > 0 and annual_cash_yield > 0:
                cash_balance *= (1.0 + annual_cash_yield) ** (days_delta / 365.0)
            elif days_delta > 0 and cash_balance < 0 and annual_borrow_rate > 0:
                # Negative cash represents borrowed funds; debt grows with borrow interest.
                cash_balance *= (1.0 + annual_borrow_rate) ** (days_delta / 365.0)
        prev_dt = dt

        for ticker in tickers:
            px = row.get(ticker)
            stt = state[ticker]

            if pd.isna(px):
                continue

            price = float(px)
            stt["last_price"] = price
            stt["history"].append(price)
            hist = stt["history"]

            buy_ret = None
            sell_ret = None
            if len(hist) > buy_window_days:
                buy_ret = ((hist[-1] / hist[-1 - buy_window_days]) - 1.0) * 100.0
            if len(hist) > sell_window_days:
                sell_ret = ((hist[-1] / hist[-1 - sell_window_days]) - 1.0) * 100.0

            if stt["shares"] <= 0:
                can_reenter = allow_reentry or stt["buy_count"] == 0
                buy_signal = buy_ret is not None and buy_ret >= buy_threshold_pct
                if can_reenter and buy_signal:
                    buy_one_unit(ticker, dt, price, buy_ret, buy_window_days)
            else:
                sell_signal = False
                if sell_ret is not None:
                    if sell_mode == "Sell on drop":
                        sell_signal = sell_ret <= -abs(sell_threshold_pct)
                    else:
                        sell_signal = sell_ret >= abs(sell_threshold_pct)

                buy_signal = buy_ret is not None and buy_ret >= buy_threshold_pct
                if sell_signal:
                    shares_before = stt["shares"]
                    cash_before = cash_balance
                    gross = shares_before * price
                    net = gross * fee_sell
                    cash_balance = cash_before + net
                    stt["shares"] = 0.0
                    stt["sell_count"] += 1
                    stt["proceeds_sold"] += net
                    trades.append(
                        {
                            "dt": dt,
                            "ticker": ticker,
                            "action": "SELL",
                            "price": price,
                            "shares": shares_before,
                            "proceeds_net": net,
                            "cash_before": cash_before,
                            "cash_after": cash_balance,
                            "shares_after": stt["shares"],
                            "signal_return_pct": sell_ret,
                            "signal_window_days": sell_window_days,
                        }
                    )
                elif buy_signal:
                    # Pyramiding behavior: keep adding one unit while trend remains valid.
                    buy_one_unit(ticker, dt, price, buy_ret, buy_window_days)

        invested_value = 0.0
        for ticker in tickers:
            stt = state[ticker]
            last_px = stt["last_price"] if stt["last_price"] is not None else 0.0
            position_value = stt["shares"] * last_px
            invested_value += position_value
        total_wealth = cash_balance + invested_value
        equity_curve.append(
            {
                "dt": dt,
                "cash_balance": cash_balance,
                "portfolio_value": invested_value,
                "deployed_value": invested_value,
                "total_wealth": total_wealth,
            }
        )

    final_rows: list[dict] = []
    for ticker in tickers:
        stt = state[ticker]
        last_px = stt["last_price"] if stt["last_price"] is not None else 0.0
        position_value = stt["shares"] * last_px
        unit_usd = float(buy_unit_by_ticker[ticker])
        final_rows.append(
            {
                "ticker": ticker,
                "buy_unit_usd": unit_usd,
                "last_price": last_px,
                "ending_shares": stt["shares"],
                "position_value": position_value,
                "notional_bought": stt["notional_bought"],
                "proceeds_sold": stt["proceeds_sold"],
                "net_flow": stt["proceeds_sold"] - stt["notional_bought"],
                "buys": stt["buy_count"],
                "sells": stt["sell_count"],
            }
        )

    trades_df = pd.DataFrame(trades)
    if not trades_df.empty:
        trades_df["dt"] = pd.to_datetime(trades_df["dt"])
        trades_df = trades_df.sort_values(["dt", "ticker", "action"]).reset_index(drop=True)

    equity_df = pd.DataFrame(equity_curve)
    if not equity_df.empty:
        equity_df["dt"] = pd.to_datetime(equity_df["dt"])

    final_df = pd.DataFrame(final_rows).sort_values("position_value", ascending=False).reset_index(drop=True)
    return trades_df, final_df, equity_df


def build_float_grid(start: float, end: float, step: float) -> list[float]:
    if step <= 0:
        return [float(start)]
    vals: list[float] = []
    x = float(start)
    guard = 0
    while x <= float(end) + 1e-9 and guard < 5000:
        vals.append(round(x, 6))
        x += float(step)
        guard += 1
    return vals


def build_int_grid(start: int, end: int, step: int) -> list[int]:
    if step <= 0:
        return [int(start)]
    return list(range(int(start), int(end) + 1, int(step)))


def run_grid_search(
    price_panel: pd.DataFrame,
    buy_unit_by_ticker: dict[str, float],
    starting_cash: float,
    annual_cash_yield_pct: float,
    annual_borrow_rate_pct: float,
    allow_leverage: bool,
    buy_threshold_values: Sequence[float],
    buy_window_values: Sequence[int],
    sell_threshold_values: Sequence[float],
    sell_window_values: Sequence[int],
    sell_mode: str,
    fee_bps: float,
    allow_reentry: bool,
    progress_callback: Callable[[int, int], None] | None = None,
) -> pd.DataFrame:
    rows: list[dict] = []
    combos = list(
        product(
            buy_threshold_values,
            buy_window_values,
            sell_threshold_values,
            sell_window_values,
        )
    )
    total = len(combos)

    for idx, (buy_th, buy_win, sell_th, sell_win) in enumerate(combos, start=1):
        trades_df, _, equity_df = run_threshold_simulation(
            price_panel=price_panel,
            buy_unit_by_ticker=buy_unit_by_ticker,
            starting_cash=float(starting_cash),
            annual_cash_yield_pct=float(annual_cash_yield_pct),
            annual_borrow_rate_pct=float(annual_borrow_rate_pct),
            allow_leverage=allow_leverage,
            buy_threshold_pct=float(buy_th),
            buy_window_days=int(buy_win),
            sell_threshold_pct=float(sell_th),
            sell_window_days=int(sell_win),
            sell_mode=sell_mode,
            fee_bps=float(fee_bps),
            allow_reentry=allow_reentry,
        )

        if equity_df.empty:
            final_cash = float(starting_cash)
            final_invested = 0.0
            final_total = final_cash
        else:
            last = equity_df.iloc[-1]
            final_cash = float(last["cash_balance"])
            final_invested = float(last["portfolio_value"])
            final_total = float(last["total_wealth"])
        pnl = final_total - float(starting_cash)
        total_return_pct = (pnl / float(starting_cash)) * 100.0 if float(starting_cash) > 0 else None
        trade_count = 0 if trades_df.empty else int(len(trades_df))

        max_drawdown_pct = None
        if not equity_df.empty and equity_df["total_wealth"].notna().any():
            eq = equity_df["total_wealth"].astype(float)
            running_max = eq.cummax()
            dd = (eq / running_max) - 1.0
            max_drawdown_pct = float(dd.min() * 100.0)

        rows.append(
            {
                "buy_threshold_pct": float(buy_th),
                "buy_window_days": int(buy_win),
                "sell_threshold_pct": float(sell_th),
                "sell_window_days": int(sell_win),
                "starting_cash": float(starting_cash),
                "final_cash": final_cash,
                "final_invested": final_invested,
                "final_total_wealth": final_total,
                "pnl": pnl,
                "total_return_pct": total_return_pct,
                "max_drawdown_pct": max_drawdown_pct,
                "trade_count": trade_count,
            }
        )
        if progress_callback:
            progress_callback(idx, total)

    out = pd.DataFrame(rows)
    if not out.empty:
        out = out.sort_values(
            ["final_total_wealth", "max_drawdown_pct", "trade_count"],
            ascending=[False, False, False],
            na_position="last",
        ).reset_index(drop=True)
    return out


# -----------------------------
# UI
# -----------------------------
inject_custom_css()
st.title("ETF Dashboard (Sector → Subsector → Ticker)")
st.markdown(
    """
    <div class="hero">
      <p class="hero-title">Market Dashboard</p>
      <p class="hero-subtitle">
        Explore sector structure, build custom baskets, and monitor short-horizon winners with a cleaner signal-first layout.
      </p>
    </div>
    """,
    unsafe_allow_html=True,
)
page = st.sidebar.radio("Page", ["Explorer", "Top Performers", "Strategy Simulator"], index=0)

if page == "Explorer":
    _, _, tickers = render_classification_filters("explorer")
    default_selection = tickers[: min(8, len(tickers))]
    selected_tickers = st.multiselect("Tickers", tickers, default=default_selection)

    c1, c2 = st.columns(2)
    with c1:
        start = st.date_input("Start date", value=date(date.today().year - 1, 1, 1), key="explorer_start")
    with c2:
        end = st.date_input("End date", value=date.today(), key="explorer_end")

    if start >= end:
        st.error("Start date must be before end date.")
        st.stop()

    if not selected_tickers:
        st.info("Pick at least one ticker.")
        st.stop()

    df = fetch_normalized_series(selected_tickers, start, end)
    if df.empty:
        st.warning("No data found for that selection/date range (did you backfill these tickers?).")
        st.stop()

    m1, m2, m3 = st.columns(3)
    with m1:
        st.metric("Universe Size", f"{len(tickers)}")
    with m2:
        st.metric("Selected", f"{len(selected_tickers)}")
    with m3:
        st.metric("Window (days)", f"{(end - start).days}")

    st.subheader("Normalized performance (starts at 1.0)")
    fig = px.line(df, x="dt", y="norm_close", color="ticker")
    st.plotly_chart(style_figure(fig), use_container_width=True)

    st.subheader("Summary")
    last = (
        df.sort_values(["ticker", "dt"])
        .groupby("ticker", as_index=False)
        .tail(1)
        .loc[:, ["ticker", "dt", "close", "norm_close"]]
        .rename(columns={"dt": "last_dt", "close": "last_close", "norm_close": "norm_close_last"})
    )

    last["period_return_%"] = (last["norm_close_last"] - 1.0) * 100.0
    last = last.sort_values("period_return_%", ascending=False)
    st.dataframe(last, use_container_width=True)

    st.divider()
    st.subheader("Add / Update custom Sector/Subsector + Build Basket")

    with st.form("custom_subsector_form"):
        new_sector = st.text_input("Sector name", value="Custom")
        new_subsector = st.text_input("Subsector name", value="My Basket")
        new_tickers = st.text_area(
            "Tickers (comma or newline separated)",
            value="AAPL, MSFT, NVDA",
            help="These tickers will be inserted into instrument (if missing) and linked to the subsector.",
        )
        weight_method = st.selectbox("Basket weighting", ["equal", "market_cap"], index=0)
        make_primary = st.checkbox(
            "Mark classification as primary?",
            value=False,
            help="If checked, sets instrument_classification.is_primary for (ticker, subsector).",
        )
        build_basket = st.form_submit_button("Save + Build Basket")

    if build_basket:
        tickers_list = [t.strip().upper() for t in new_tickers.replace("\n", ",").split(",") if t.strip()]

        if not tickers_list:
            st.error("Please provide at least one ticker.")
            st.stop()

        try:
            conn = get_conn()
            subsector_id, basket_df, weights_df, warn = create_or_update_subsector_basket(
                conn,
                sector_name=new_sector.strip(),
                subsector_name=new_subsector.strip(),
                tickers=tickers_list,
                weight_method=weight_method,
                start=start,
                end=end,
                is_primary=make_primary,
            )
        except Exception as e:
            st.exception(e)
            st.stop()

        st.success(f"Saved. subsector_id = {subsector_id}")

        if warn:
            st.warning(warn)

        st.write("Weights used:")
        st.dataframe(weights_df.sort_values("w", ascending=False), use_container_width=True)

        if basket_df.empty:
            st.warning("No basket price data found (missing prices_1d backfill for these tickers?).")
        else:
            st.subheader("Basket (weighted, then normalized to 1.0 at start)")
            fig2 = px.line(basket_df, x="dt", y="basket_norm", title="Basket normalized performance")
            st.plotly_chart(style_figure(fig2), use_container_width=True)

            st.caption(
                "Basket is computed as Σ(wᵢ·closeᵢ) per day, then normalized by its first value "
                "in the selected period."
            )
elif page == "Top Performers":
    _, _, universe_tickers = render_classification_filters("top")
    st.caption("Rankings use tickers from the selected sector/subsector universe.")

    if not universe_tickers:
        st.info("No tickers available for this filter selection.")
        st.stop()

    top_n = st.slider("Top N", min_value=3, max_value=20, value=5, step=1)
    st.metric("Ranking Universe", f"{len(universe_tickers)} tickers")
    d1, d2 = st.columns(2)
    with d1:
        render_top_performer_block("Top performers - last 7 days", 7, universe_tickers, top_n)
    with d2:
        render_top_performer_block("Top performers - last 30 days", 30, universe_tickers, top_n)
else:
    _, _, sim_universe = render_classification_filters("sim")
    st.caption(
        "Simulates rule-based buys/sells across selected ETFs with one shared cash pool using daily closes. "
        "Buy rule: price rises by threshold over buy window. "
        "Sell rule: drop or gain threshold over sell window. "
        "When already invested, a new buy signal adds one more unit. "
        "Idle cash compounds at the configured annual cash yield, and negative cash pays borrow interest."
    )

    if not sim_universe:
        st.info("No tickers available for this filter selection.")
        st.stop()

    default_sim_selection = sim_universe[: min(12, len(sim_universe))]
    sim_tickers = st.multiselect(
        "Tickers to simulate",
        sim_universe,
        default=default_sim_selection,
        key="sim_tickers",
    )
    if not sim_tickers:
        st.info("Pick at least one ticker to run the simulation.")
        st.stop()

    c1, c2 = st.columns(2)
    with c1:
        sim_start = st.date_input("Simulation start", value=date(date.today().year - 1, 1, 1), key="sim_start")
    with c2:
        sim_end = st.date_input("Simulation end", value=date.today(), key="sim_end")

    if sim_start >= sim_end:
        st.error("Simulation start must be before end.")
        st.stop()

    r1, r2, r3 = st.columns(3)
    with r1:
        buy_window = st.slider("Buy lookback (days)", min_value=2, max_value=60, value=5, step=1)
        buy_threshold = st.number_input("Buy threshold %", min_value=0.1, max_value=50.0, value=2.0, step=0.1)
    with r2:
        sell_window = st.slider("Sell lookback (days)", min_value=2, max_value=60, value=5, step=1)
        sell_threshold = st.number_input("Sell threshold %", min_value=0.1, max_value=50.0, value=2.0, step=0.1)
    with r3:
        sell_mode = st.selectbox("Sell trigger", ["Sell on drop", "Sell on gain"], index=0)
        fee_bps = st.number_input("Trading fee (bps)", min_value=0.0, max_value=200.0, value=0.0, step=0.5)
        allow_reentry = st.checkbox("Allow re-entry after selling", value=True)

    c_cash_1, c_cash_2, c_cash_3 = st.columns(3)
    with c_cash_1:
        starting_cash = st.number_input(
            "Starting cash (USD)",
            min_value=0.0,
            max_value=100_000_000.0,
            value=100_000.0,
            step=1000.0,
        )
    with c_cash_2:
        annual_cash_yield_pct = st.number_input(
            "Cash yield annual %", min_value=0.0, max_value=25.0, value=2.0, step=0.1
        )
    with c_cash_3:
        annual_borrow_rate_pct = st.number_input(
            "Borrow rate annual %", min_value=0.0, max_value=50.0, value=4.0, step=0.1
        )
    allow_leverage = st.checkbox("Allow leverage (cash can go negative)", value=True)

    deployment_mode = st.radio(
        "Deployment sizing",
        ["Single amount for all tickers", "Per-ticker amounts"],
        horizontal=True,
        key="sim_deployment_mode",
    )

    if deployment_mode == "Single amount for all tickers":
        deployment_per_trade = st.number_input(
            "Deployment per trade (USD)",
            min_value=0.0,
            max_value=1_000_000.0,
            value=1000.0,
            step=100.0,
        )
        buy_unit_by_ticker = {t: float(deployment_per_trade) for t in sim_tickers}
        st.caption("Each buy signal deploys this amount for the triggered ticker.")
    else:
        default_initial = st.number_input(
            "Default unit amount per ETF buy (USD)",
            min_value=0.0,
            max_value=1_000_000.0,
            value=1000.0,
            step=100.0,
        )
        st.caption("This default auto-fills new tickers. Use the button below to overwrite all selected tickers.")

        if "sim_alloc_map" not in st.session_state:
            st.session_state["sim_alloc_map"] = {}
        alloc_map: dict[str, float] = st.session_state["sim_alloc_map"]
        for t in sim_tickers:
            if t not in alloc_map:
                alloc_map[t] = float(default_initial)
        if st.button("Apply default to all selected tickers", key="sim_apply_default_units"):
            for t in sim_tickers:
                alloc_map[t] = float(default_initial)

        alloc_df = pd.DataFrame(
            {
                "ticker": sim_tickers,
                "buy_unit_usd": [float(alloc_map[t]) for t in sim_tickers],
            }
        )
        st.subheader("Per-ticker buy unit")
        edited_alloc = st.data_editor(
            alloc_df,
            hide_index=True,
            use_container_width=True,
            disabled=["ticker"],
            column_config={
                "ticker": st.column_config.TextColumn("Ticker"),
                "buy_unit_usd": st.column_config.NumberColumn("Buy Unit USD", min_value=0.0, step=100.0),
            },
            key="sim_alloc_editor",
        )
        for _, row in edited_alloc.iterrows():
            alloc_map[str(row["ticker"])] = float(max(0.0, row["buy_unit_usd"]))

        buy_unit_by_ticker = {t: float(alloc_map.get(t, default_initial)) for t in sim_tickers}

    if sum(buy_unit_by_ticker.values()) <= 0:
        st.warning("Total buy-unit configuration must be greater than zero.")
        st.stop()
    if starting_cash <= 0:
        st.warning("Starting cash must be greater than zero.")
        st.stop()

    price_panel = fetch_close_panel(sim_tickers, sim_start, sim_end)
    if price_panel.empty:
        st.warning("No daily price data found for this selection/date range.")
        st.stop()

    trades_df, final_df, equity_df = run_threshold_simulation(
        price_panel=price_panel,
        buy_unit_by_ticker=buy_unit_by_ticker,
        starting_cash=float(starting_cash),
        annual_cash_yield_pct=float(annual_cash_yield_pct),
        annual_borrow_rate_pct=float(annual_borrow_rate_pct),
        allow_leverage=allow_leverage,
        buy_threshold_pct=float(buy_threshold),
        buy_window_days=int(buy_window),
        sell_threshold_pct=float(sell_threshold),
        sell_window_days=int(sell_window),
        sell_mode=sell_mode,
        fee_bps=float(fee_bps),
        allow_reentry=allow_reentry,
    )

    base_units_total = float(final_df["buy_unit_usd"].sum())
    if equity_df.empty:
        final_cash = float(starting_cash)
        final_invested = 0.0
        final_total = final_cash
    else:
        last_equity = equity_df.iloc[-1]
        final_cash = float(last_equity["cash_balance"])
        final_invested = float(last_equity["portfolio_value"])
        final_total = float(last_equity["total_wealth"])
    pnl_total = final_total - float(starting_cash)
    total_return_pct = (pnl_total / float(starting_cash)) * 100.0 if float(starting_cash) > 0 else 0.0

    k1, k2, k3, k4, k5, k6 = st.columns(6)
    with k1:
        st.metric("Starting Cash", f"${starting_cash:,.2f}")
    with k2:
        st.metric("Final Cash", f"${final_cash:,.2f}")
    with k3:
        st.metric("Final Portfolio", f"${final_invested:,.2f}")
    with k4:
        st.metric("Final Total Wealth", f"${final_total:,.2f}")
    with k5:
        st.metric("PnL", f"${pnl_total:,.2f}")
    with k6:
        st.metric("Total Return %", f"{total_return_pct:.2f}%")
    st.caption(f"Configured buy-unit total across tickers: ${base_units_total:,.2f}")
    st.metric("Trades", f"{0 if trades_df.empty else len(trades_df)}")

    st.subheader("Final holdings")
    st.dataframe(final_df, use_container_width=True)

    if not equity_df.empty:
        curve_df = equity_df.melt(
            id_vars=["dt"],
            value_vars=["cash_balance", "portfolio_value", "total_wealth"],
            var_name="curve",
            value_name="value",
        )
        curve_df["curve"] = curve_df["curve"].map(
            {
                "cash_balance": "Cash",
                "portfolio_value": "Portfolio (Invested)",
                "total_wealth": "Total Wealth",
            }
        )
        eq_fig = px.line(curve_df, x="dt", y="value", color="curve", title="Cash, portfolio, and total wealth over time")
        st.plotly_chart(style_figure(eq_fig), use_container_width=True)

    st.subheader("Trade log (all buys and sells)")
    if trades_df.empty:
        st.info("No trades were triggered with current thresholds.")
    else:
        st.dataframe(trades_df, use_container_width=True)

    st.divider()
    st.subheader("Grid Search - Best 4-Lever Combination")
    st.caption(
        "Runs a parameter sweep over buy threshold/window and sell threshold/window. "
        "Objective: maximize final total wealth (cash + portfolio)."
    )

    g1, g2, g3, g4 = st.columns(4)
    with g1:
        buy_th_min = st.number_input("Buy threshold min %", min_value=0.1, max_value=50.0, value=1.0, step=0.1)
        buy_th_max = st.number_input("Buy threshold max %", min_value=0.1, max_value=50.0, value=5.0, step=0.1)
        buy_th_step = st.number_input("Buy threshold step %", min_value=0.1, max_value=10.0, value=1.0, step=0.1)
    with g2:
        buy_win_min = st.number_input("Buy window min (days)", min_value=2, max_value=120, value=3, step=1)
        buy_win_max = st.number_input("Buy window max (days)", min_value=2, max_value=120, value=12, step=1)
        buy_win_step = st.number_input("Buy window step", min_value=1, max_value=30, value=3, step=1)
    with g3:
        sell_th_min = st.number_input("Sell threshold min %", min_value=0.1, max_value=50.0, value=1.0, step=0.1)
        sell_th_max = st.number_input("Sell threshold max %", min_value=0.1, max_value=50.0, value=5.0, step=0.1)
        sell_th_step = st.number_input("Sell threshold step %", min_value=0.1, max_value=10.0, value=1.0, step=0.1)
    with g4:
        sell_win_min = st.number_input("Sell window min (days)", min_value=2, max_value=120, value=3, step=1)
        sell_win_max = st.number_input("Sell window max (days)", min_value=2, max_value=120, value=12, step=1)
        sell_win_step = st.number_input("Sell window step", min_value=1, max_value=30, value=3, step=1)

    if buy_th_min > buy_th_max or buy_win_min > buy_win_max or sell_th_min > sell_th_max or sell_win_min > sell_win_max:
        st.error("Each min value must be <= max value for grid search.")
        st.stop()

    buy_th_values = build_float_grid(buy_th_min, buy_th_max, buy_th_step)
    buy_win_values = build_int_grid(int(buy_win_min), int(buy_win_max), int(buy_win_step))
    sell_th_values = build_float_grid(sell_th_min, sell_th_max, sell_th_step)
    sell_win_values = build_int_grid(int(sell_win_min), int(sell_win_max), int(sell_win_step))

    combo_count = len(buy_th_values) * len(buy_win_values) * len(sell_th_values) * len(sell_win_values)
    st.caption(f"Combinations to evaluate: {combo_count}")
    if combo_count > 400:
        st.warning("Large grid. Consider tightening ranges for faster results.")

    run_grid = st.button("Run Grid Search", type="primary")
    if run_grid:
        progress_text = st.empty()
        progress_bar = st.progress(0)

        def on_progress(done: int, total: int) -> None:
            pct = int((done / total) * 100) if total > 0 else 0
            progress_text.caption(f"Grid progress: {done} / {total}")
            progress_bar.progress(pct)

        with st.spinner("Evaluating parameter combinations..."):
            grid_df = run_grid_search(
                price_panel=price_panel,
                buy_unit_by_ticker=buy_unit_by_ticker,
                starting_cash=float(starting_cash),
                annual_cash_yield_pct=float(annual_cash_yield_pct),
                annual_borrow_rate_pct=float(annual_borrow_rate_pct),
                allow_leverage=allow_leverage,
                buy_threshold_values=buy_th_values,
                buy_window_values=buy_win_values,
                sell_threshold_values=sell_th_values,
                sell_window_values=sell_win_values,
                sell_mode=sell_mode,
                fee_bps=float(fee_bps),
                allow_reentry=allow_reentry,
                progress_callback=on_progress,
            )
        progress_text.caption(f"Grid progress: {combo_count} / {combo_count}")
        progress_bar.progress(100)

        if grid_df.empty:
            st.warning("Grid search returned no results.")
        else:
            best = grid_df.iloc[0]
            b1, b2, b3, b4, b5 = st.columns(5)
            with b1:
                st.metric("Best Final Wealth", f"${best['final_total_wealth']:,.2f}")
            with b2:
                st.metric("Best Buy Lever", f"{best['buy_threshold_pct']:.2f}% / {int(best['buy_window_days'])}d")
            with b3:
                st.metric("Best Sell Lever", f"{best['sell_threshold_pct']:.2f}% / {int(best['sell_window_days'])}d")
            with b4:
                st.metric("Trades (best)", f"{int(best['trade_count'])}")
            with b5:
                best_ret = best.get("total_return_pct")
                st.metric("Total Return %", "n/a" if pd.isna(best_ret) else f"{best_ret:.2f}%")

            st.dataframe(grid_df.head(30), use_container_width=True)
