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
import html
from datetime import date, datetime, timedelta, timezone
from itertools import product
from typing import Sequence, Literal, Tuple, Callable
from zoneinfo import ZoneInfo

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
    "#0f62fe",
    "#0f766e",
    "#1f2937",
    "#b45309",
    "#dc2626",
    "#0891b2",
    "#65a30d",
    "#334155",
]


def inject_custom_css() -> None:
    st.markdown(
        """
        <style>
          @import url('https://fonts.googleapis.com/css2?family=Nunito:wght@400;500;600;700;800&family=DM+Sans:wght@500;600;700&display=swap');

          :root {
            --shell-bg: #d3d4e0;
            --sidebar-bg: #334a69;
            --sidebar-bg-strong: #2e4461;
            --sidebar-text: #e6edf6;
            --sidebar-muted: #b5c3d7;
            --canvas-bg: #eceff4;
            --canvas-border: #c9d1dd;
            --card-bg: #f8f9fc;
            --card-border: #d5dbe5;
            --text-main: #273852;
            --text-muted: #75839a;
            --accent: #31598f;
            --accent-soft: rgba(49, 89, 143, 0.16);
          }

          .stApp {
            background: var(--shell-bg);
            color: var(--text-main);
            font-family: "Nunito", "Segoe UI", sans-serif;
          }

          [data-testid="stHeader"] {
            background: transparent;
          }

          #MainMenu, footer {
            visibility: hidden;
          }

          main .block-container {
            position: relative;
            overflow: hidden;
            max-width: 1460px;
            margin-top: 0.85rem;
            margin-bottom: 1rem;
            background: var(--canvas-bg);
            border: 1px solid var(--canvas-border);
            border-radius: 18px;
            box-shadow: 0 16px 38px rgba(24, 40, 66, 0.12);
            padding: 1.55rem 1.25rem 2.1rem 1.25rem;
          }

          main .block-container::before {
            content: "";
            position: absolute;
            left: 0;
            right: 0;
            top: 0;
            height: 14px;
            background: #324c70;
          }

          [data-testid="stSidebar"] {
            background: linear-gradient(180deg, var(--sidebar-bg) 0%, var(--sidebar-bg-strong) 100%);
            border-right: none;
          }

          [data-testid="stSidebar"] .block-container {
            padding-top: 1.1rem;
            padding-left: 1rem;
            padding-right: 1rem;
          }

          .sidebar-brand {
            display: flex;
            align-items: center;
            gap: 0.58rem;
            margin: 0.12rem 0 1rem 0;
          }

          .sidebar-badge {
            width: 34px;
            height: 34px;
            border-radius: 50%;
            display: inline-flex;
            align-items: center;
            justify-content: center;
            border: 1px solid rgba(255, 255, 255, 0.35);
            color: #f6fbff;
            font-size: 0.88rem;
          }

          .sidebar-brand-text {
            color: #f2f7ff;
            font-size: 1.06rem;
            font-weight: 700;
            letter-spacing: -0.01em;
          }

          .sidebar-section {
            color: var(--sidebar-muted);
            font-size: 0.69rem;
            font-weight: 700;
            text-transform: uppercase;
            letter-spacing: 0.08em;
            margin: 1.05rem 0 0.45rem 0.08rem;
          }

          .sidebar-item {
            color: var(--sidebar-text);
            border-radius: 9px;
            padding: 0.38rem 0.62rem;
            margin: 0.16rem 0;
            font-weight: 600;
            font-size: 0.99rem;
            background: transparent;
          }

          .sidebar-item.muted {
            color: #d6dfec;
            opacity: 0.86;
          }

          [data-testid="stSidebar"] .stRadio > label {
            display: none;
          }

          [data-testid="stSidebar"] .stRadio > div {
            gap: 0.34rem;
          }

          [data-testid="stSidebar"] .stRadio label {
            border-radius: 10px;
            padding: 0.34rem 0.46rem;
            margin: 0;
            border: 1px solid transparent;
            background: transparent;
          }

          [data-testid="stSidebar"] .stRadio label:hover {
            background: rgba(255, 255, 255, 0.06);
          }

          [data-testid="stSidebar"] .stRadio label:has(input:checked) {
            background: rgba(255, 255, 255, 0.12);
            border-color: rgba(255, 255, 255, 0.09);
          }

          [data-testid="stSidebar"] .stRadio p {
            color: #e8eef8;
            margin: 0;
            font-weight: 700;
            font-size: 1.02rem;
            letter-spacing: -0.01em;
          }

          h1, h2, h3 {
            font-family: "DM Sans", "Nunito", sans-serif;
            letter-spacing: -0.02em;
            color: #2a4266;
            font-weight: 700;
          }

          h1 {
            font-size: 1.5rem;
            margin-bottom: 0.15rem;
          }

          .hero {
            border-radius: 12px;
            border: 1px solid var(--card-border);
            background: #f8fafd;
            margin: 0.25rem 0 0.85rem 0;
            overflow: hidden;
          }

          .hero-head {
            display: flex;
            align-items: center;
            gap: 0.55rem;
            padding: 0.64rem 0.95rem 0.58rem 0.95rem;
            background: #f5f7fb;
            border-bottom: 1px solid var(--card-border);
          }

          .hero-icon {
            width: 18px;
            height: 18px;
            border-radius: 6px;
            background:
              radial-gradient(circle at 30% 30%, #335f96 0 24%, transparent 25%),
              radial-gradient(circle at 70% 30%, #335f96 0 24%, transparent 25%),
              radial-gradient(circle at 30% 70%, #335f96 0 24%, transparent 25%),
              radial-gradient(circle at 70% 70%, #335f96 0 24%, transparent 25%);
          }

          .hero-title {
            margin: 0;
            color: #32527f;
            font-family: "DM Sans", "Nunito", sans-serif;
            font-size: 1.04rem;
            font-weight: 700;
            letter-spacing: -0.01em;
          }

          .hero-subtitle {
            margin: 0;
            padding: 0.54rem 0.95rem 0.58rem 0.95rem;
            color: #7b8899;
            font-size: 0.83rem;
            font-weight: 600;
          }

          [data-testid="stMetric"] {
            border: 1px solid var(--card-border);
            border-radius: 12px;
            background: var(--card-bg);
            box-shadow: 0 1px 1px rgba(31, 45, 71, 0.04);
            padding: 0.56rem 0.7rem;
          }

          [data-testid="stMetricLabel"] {
            color: #7a8798;
            font-weight: 700;
            font-size: 0.78rem;
            letter-spacing: 0.01em;
          }

          [data-testid="stMetricValue"] {
            color: #1f334f;
            font-family: "DM Sans", "Nunito", sans-serif;
            font-size: 1.66rem;
            font-weight: 800;
            letter-spacing: -0.02em;
          }

          [data-testid="stDataFrame"],
          [data-testid="stTable"],
          [data-testid="stForm"],
          [data-testid="stAlert"] {
            border: 1px solid var(--card-border);
            border-radius: 12px;
            overflow: hidden;
            background: #f8fafd;
            box-shadow: 0 1px 2px rgba(19, 35, 58, 0.05);
          }

          [data-testid="stForm"] {
            background: #f7f9fc;
            padding: 0.76rem 0.84rem 0.62rem 0.84rem;
          }

          [data-testid="stAlert"] {
            border-left: 3px solid var(--accent);
          }

          [data-baseweb="select"] > div,
          .stTextInput > div > div > input,
          .stDateInput > div > div input,
          .stNumberInput > div > div > input,
          .stMultiSelect > div > div,
          .stTextArea > div > div > textarea {
            border-radius: 10px;
            border: 1px solid #cfd5df;
            background: #fdfdff;
            font-family: "Nunito", "Segoe UI", sans-serif;
          }

          [data-baseweb="select"] > div:focus-within,
          .stTextInput > div > div > input:focus,
          .stDateInput > div > div input:focus,
          .stNumberInput > div > div > input:focus,
          .stTextArea > div > div > textarea:focus {
            border-color: rgba(49, 89, 143, 0.52);
            box-shadow: 0 0 0 3px rgba(49, 89, 143, 0.13);
          }

          .stButton > button,
          div[data-testid="stFormSubmitButton"] button {
            background: #335786;
            color: #f4f8ff;
            border: 1px solid #2d4f7b;
            border-radius: 10px;
            font-weight: 700;
            box-shadow: 0 8px 14px rgba(40, 66, 101, 0.18);
          }

          .stButton > button:hover,
          div[data-testid="stFormSubmitButton"] button:hover {
            background: #294a74;
            border-color: #27466d;
          }

          .stTabs [role="tablist"] {
            gap: 0.3rem;
          }

          .stTabs [role="tab"] {
            border-radius: 9px;
            border: 1px solid #cfd5df;
            background: #f6f8fb;
            color: #506079;
            font-weight: 700;
            padding: 0.34rem 0.8rem;
          }

          .stTabs [role="tab"][aria-selected="true"] {
            background: #395a86;
            border-color: #395a86;
            color: #f3f8ff;
          }

          .stProgress > div > div > div > div {
            background: #3f6ea9;
          }

          .overview-kpi-grid {
            display: grid;
            grid-template-columns: repeat(4, minmax(0, 1fr));
            gap: 0.86rem;
            margin: 0.22rem 0 1.05rem 0;
          }

          .overview-kpi-card {
            border: 1px solid var(--card-border);
            border-radius: 14px;
            background: #fdfdff;
            box-shadow: 0 2px 4px rgba(37, 58, 88, 0.08);
            overflow: hidden;
          }

          .overview-kpi-top {
            display: flex;
            align-items: center;
            justify-content: space-between;
            gap: 0.4rem;
            padding: 0.72rem 0.86rem 0.7rem 0.86rem;
            border-bottom: 1px solid #dce2ec;
          }

          .overview-kpi-label-wrap {
            display: flex;
            align-items: center;
            gap: 0.58rem;
          }

          .overview-kpi-icon {
            width: 34px;
            height: 34px;
            border-radius: 10px;
            display: inline-flex;
            align-items: center;
            justify-content: center;
            font-size: 0.96rem;
            color: #2b4f7d;
            border: 1px solid rgba(54, 86, 129, 0.26);
            background: #edf3fb;
          }

          .overview-kpi-label {
            font-size: 0.75rem;
            font-weight: 800;
            letter-spacing: 0.01em;
            color: #1f3554;
            margin: 0;
          }

          .overview-kpi-chevron {
            color: #8f9db2;
            font-size: 1.03rem;
            font-weight: 700;
          }

          .overview-kpi-bottom {
            display: flex;
            align-items: flex-end;
            justify-content: space-between;
            gap: 0.8rem;
            padding: 0.7rem 0.86rem 0.8rem 0.86rem;
          }

          .overview-kpi-value {
            font-family: "DM Sans", "Nunito", sans-serif;
            font-size: 2rem;
            line-height: 1;
            font-weight: 700;
            letter-spacing: -0.03em;
            color: #1a2c44;
            margin: 0;
          }

          .overview-kpi-subtitle {
            margin: 0.2rem 0 0 0;
            color: #7f8b9b;
            font-size: 0.81rem;
            font-weight: 700;
          }

          .overview-kpi-bars {
            display: flex;
            align-items: flex-end;
            gap: 0.28rem;
            min-height: 48px;
          }

          .overview-kpi-bars span {
            width: 11px;
            border-radius: 3px 3px 2px 2px;
            background: rgba(61, 92, 132, 0.9);
          }

          .overview-grid {
            display: grid;
            grid-template-columns: minmax(340px, 1fr) minmax(380px, 1.45fr);
            gap: 0.95rem;
            margin-bottom: 1.05rem;
          }

          .overview-panel {
            border: 1px solid var(--card-border);
            border-radius: 14px;
            background: #fdfdff;
            overflow: hidden;
            box-shadow: 0 2px 5px rgba(29, 45, 67, 0.08);
            min-height: 484px;
          }

          .overview-panel-head {
            display: flex;
            align-items: center;
            justify-content: space-between;
            padding: 0.8rem 0.95rem;
            border-bottom: 1px solid #dde3ed;
            background: #f4f7fc;
          }

          .overview-panel-title {
            margin: 0;
            font-family: "DM Sans", "Nunito", sans-serif;
            font-size: 1.06rem;
            font-weight: 700;
            color: #2b4e7c;
            letter-spacing: -0.01em;
          }

          .overview-panel-chip {
            width: 38px;
            height: 38px;
            border-radius: 12px;
            border: 1px solid #cfdaea;
            background: #edf3fb;
            color: #2f5788;
            display: inline-flex;
            align-items: center;
            justify-content: center;
            font-size: 1rem;
            font-weight: 700;
          }

          .overview-panel-body {
            padding: 0.92rem 0.95rem 1rem 0.95rem;
          }

          .overview-section-label {
            margin: 0;
            font-size: 0.72rem;
            color: #7d8998;
            font-weight: 800;
            letter-spacing: 0.07em;
            text-transform: uppercase;
          }

          .overview-progress-wrap {
            display: grid;
            grid-template-columns: 1fr 1fr;
            gap: 0.6rem;
            margin-top: 0.58rem;
            margin-bottom: 0.9rem;
          }

          .overview-progress-card {
            border: 1px solid #d9e0eb;
            border-radius: 10px;
            background: #f9fbff;
            padding: 0.56rem 0.62rem;
          }

          .overview-progress-head {
            display: flex;
            align-items: center;
            justify-content: space-between;
            gap: 0.5rem;
            margin-bottom: 0.4rem;
          }

          .overview-progress-title {
            margin: 0;
            font-size: 0.86rem;
            font-weight: 700;
            color: #37547d;
          }

          .overview-progress-value {
            margin: 0;
            font-size: 0.82rem;
            font-weight: 800;
            color: #23426c;
          }

          .overview-progress-track {
            height: 5px;
            border-radius: 100px;
            background: #dfe5f0;
            overflow: hidden;
          }

          .overview-progress-fill {
            height: 100%;
            border-radius: 100px;
            background: linear-gradient(90deg, #4b73a8 0%, #2f5688 100%);
          }

          .overview-list-head {
            display: flex;
            align-items: center;
            justify-content: space-between;
            margin-bottom: 0.74rem;
          }

          .overview-list-title {
            margin: 0;
            font-size: 1rem;
            color: #2c3f5c;
            font-weight: 800;
          }

          .overview-list-count {
            color: #456ea5;
            background: rgba(69, 110, 165, 0.14);
            border-radius: 999px;
            font-size: 0.75rem;
            font-weight: 800;
            padding: 0.16rem 0.52rem;
          }

          .overview-timeline {
            position: relative;
            margin-top: 0.2rem;
            padding-left: 0.32rem;
          }

          .overview-timeline::before {
            content: "";
            position: absolute;
            left: 55px;
            top: 6px;
            bottom: 6px;
            width: 2px;
            background: #cae0c5;
          }

          .overview-item {
            display: grid;
            grid-template-columns: 48px 14px 1fr;
            gap: 0.46rem;
            align-items: center;
            margin-bottom: 0.52rem;
          }

          .overview-time {
            font-size: 0.9rem;
            font-weight: 700;
            color: #6f7f95;
          }

          .overview-dot {
            width: 9px;
            height: 9px;
            border-radius: 50%;
            background: #7eb980;
            position: relative;
            z-index: 1;
            justify-self: center;
          }

          .overview-item-card {
            border: 1px solid #dde4ee;
            border-radius: 12px;
            background: #fcfdff;
            padding: 0.58rem 0.66rem;
            display: flex;
            align-items: center;
            justify-content: space-between;
            gap: 0.66rem;
          }

          .overview-item-main {
            min-width: 0;
          }

          .overview-item-ticker {
            margin: 0;
            font-size: 1rem;
            color: #223955;
            font-weight: 800;
          }

          .overview-item-note {
            margin: 0.08rem 0 0 0;
            font-size: 0.82rem;
            color: #7b8798;
            font-weight: 700;
            white-space: nowrap;
            overflow: hidden;
            text-overflow: ellipsis;
          }

          .overview-pill {
            border-radius: 999px;
            font-size: 0.77rem;
            font-weight: 800;
            padding: 0.2rem 0.52rem;
            white-space: nowrap;
          }

          .overview-pill.up {
            color: #1d6635;
            background: rgba(42, 146, 79, 0.16);
          }

          .overview-pill.down {
            color: #9d3137;
            background: rgba(209, 66, 78, 0.14);
          }

          .overview-leader-top {
            display: flex;
            align-items: center;
            justify-content: space-between;
            gap: 0.8rem;
            padding-bottom: 0.7rem;
            border-bottom: 1px solid #dee5ef;
            margin-bottom: 0.8rem;
          }

          .overview-avatar {
            width: 44px;
            height: 44px;
            border-radius: 50%;
            border: 1px solid #cbd8ea;
            background: radial-gradient(circle at 32% 25%, #7ea1cc 0%, #3f6699 58%, #2d4d79 100%);
            box-shadow: inset 0 0 0 5px rgba(247, 251, 255, 0.42);
            flex-shrink: 0;
          }

          .overview-leader-name {
            margin: 0;
            font-size: 1.62rem;
            letter-spacing: -0.01em;
            font-family: "DM Sans", "Nunito", sans-serif;
            color: #1d3555;
            font-weight: 700;
          }

          .overview-leader-sub {
            margin: 0.12rem 0 0 0;
            font-size: 0.9rem;
            color: #718096;
            font-weight: 700;
          }

          .overview-stats-grid {
            display: grid;
            grid-template-columns: 1fr 1fr;
            gap: 0.72rem 1rem;
            margin-bottom: 0.86rem;
          }

          .overview-stat-label {
            margin: 0;
            font-size: 0.7rem;
            color: #7c8899;
            letter-spacing: 0.06em;
            text-transform: uppercase;
            font-weight: 800;
          }

          .overview-stat-value {
            margin: 0.16rem 0 0 0;
            font-size: 1.03rem;
            color: #203956;
            font-weight: 800;
          }

          .overview-note {
            border-top: 1px solid #dfe6f1;
            padding-top: 0.72rem;
            color: #65748a;
            font-size: 0.88rem;
            font-weight: 700;
          }

          @media (max-width: 960px) {
            main .block-container {
              margin-top: 0.3rem;
              border-radius: 14px;
              padding-top: 1.2rem;
            }
            main .block-container::before {
              height: 9px;
            }
            [data-testid="stSidebar"] .block-container {
              padding-top: 0.8rem;
            }
            .overview-kpi-grid {
              grid-template-columns: repeat(2, minmax(0, 1fr));
            }
            .overview-grid {
              grid-template-columns: 1fr;
            }
            .overview-panel {
              min-height: auto;
            }
          }
        </style>
        """,
        unsafe_allow_html=True,
    )


def style_figure(fig, title: str | None = None):
    fig.update_layout(
        template="plotly_white",
        font={"family": "Nunito, Segoe UI, sans-serif", "size": 12, "color": "#273852"},
        colorway=CHART_COLORS,
        paper_bgcolor="rgba(0,0,0,0)",
        plot_bgcolor="#f9fafc",
        hovermode="x unified",
        margin={"l": 18, "r": 12, "t": 56, "b": 20},
        legend={"orientation": "h", "yanchor": "bottom", "y": 1.02, "xanchor": "left", "x": 0},
        hoverlabel={
            "bgcolor": "#314f78",
            "font": {"color": "#f1f7ff", "family": "Nunito, Segoe UI, sans-serif", "size": 12},
        },
    )
    if title:
        fig.update_layout(title={"text": title, "x": 0.01, "font": {"size": 16}})
    fig.update_xaxes(
        showgrid=False,
        zeroline=False,
        showline=True,
        linecolor="rgba(62, 82, 111, 0.28)",
        ticks="outside",
        tickcolor="rgba(62, 82, 111, 0.22)",
    )
    fig.update_yaxes(
        showgrid=True,
        gridcolor="rgba(74, 95, 127, 0.14)",
        zeroline=False,
        showline=True,
        linecolor="rgba(62, 82, 111, 0.12)",
    )
    return fig


def _safe_float(value, default: float = 0.0) -> float:
    try:
        if value is None or pd.isna(value):
            return float(default)
        return float(value)
    except Exception:
        return float(default)


def _spark_heights(values: Sequence[float], bars: int = 6) -> list[int]:
    cleaned = [_safe_float(v, 0.0) for v in values if v is not None and not pd.isna(v)]
    if not cleaned:
        return [26 for _ in range(bars)]

    if len(cleaned) < bars:
        cleaned = [cleaned[0]] * (bars - len(cleaned)) + cleaned
    elif len(cleaned) > bars:
        if bars == 1:
            cleaned = [cleaned[-1]]
        else:
            stride = (len(cleaned) - 1) / float(bars - 1)
            cleaned = [cleaned[int(round(i * stride))] for i in range(bars)]

    lo = min(cleaned)
    hi = max(cleaned)
    if abs(hi - lo) < 1e-9:
        return [26 for _ in cleaned]

    return [int(18 + ((v - lo) / (hi - lo)) * 28) for v in cleaned]


def render_overview_stat_cards(cards: Sequence[dict]) -> None:
    if not cards:
        return

    nodes: list[str] = []
    for card in cards:
        title = html.escape(str(card.get("title", "")))
        value = html.escape(str(card.get("value", "-")))
        subtitle = html.escape(str(card.get("subtitle", "")))
        icon = html.escape(str(card.get("icon", "•")))
        accent = str(card.get("accent", "#3a5f90"))
        spark_raw = card.get("spark", [])
        spark_values = spark_raw if isinstance(spark_raw, (list, tuple, pd.Series)) else []
        heights = _spark_heights(spark_values, bars=6)
        denom = max(1, len(heights) - 1)
        bars_html = "".join(
            f'<span style="height:{h}px;opacity:{0.28 + (idx / denom) * 0.7:.2f};background:{accent};"></span>'
            for idx, h in enumerate(heights)
        )
        nodes.append(
            f"""
            <article class="overview-kpi-card">
              <div class="overview-kpi-top">
                <div class="overview-kpi-label-wrap">
                  <span class="overview-kpi-icon">{icon}</span>
                  <p class="overview-kpi-label">{title}</p>
                </div>
                <span class="overview-kpi-chevron">›</span>
              </div>
              <div class="overview-kpi-bottom">
                <div>
                  <p class="overview-kpi-value">{value}</p>
                  <p class="overview-kpi-subtitle">{subtitle}</p>
                </div>
                <div class="overview-kpi-bars">{bars_html}</div>
              </div>
            </article>
            """
        )

    st.markdown(
        f'<section class="overview-kpi-grid">{"".join(nodes)}</section>',
        unsafe_allow_html=True,
    )


def _fmt_short_date(value) -> str:
    ts = pd.to_datetime(value, errors="coerce")
    if pd.isna(ts):
        return "n/a"
    return ts.strftime("%b %d, %Y")


def render_overview_queue_panel(rank_7d: pd.DataFrame, rank_30d: pd.DataFrame, max_items: int = 5) -> None:
    total_7 = len(rank_7d)
    total_30 = len(rank_30d)
    pos_7 = int((rank_7d["return_pct"] > 0).sum()) if total_7 else 0
    pos_30 = int((rank_30d["return_pct"] > 0).sum()) if total_30 else 0
    pct_7 = int(round((pos_7 / total_7) * 100)) if total_7 else 0
    pct_30 = int(round((pos_30 / total_30) * 100)) if total_30 else 0

    timeline_df = rank_7d.head(max_items) if total_7 else rank_30d.head(max_items)
    rows: list[str] = []
    for idx, row in timeline_df.reset_index(drop=True).iterrows():
        ticker = html.escape(str(row.get("ticker", "-")))
        ret = _safe_float(row.get("return_pct"), 0.0)
        start_dt = _fmt_short_date(row.get("start_dt"))
        end_dt = _fmt_short_date(row.get("latest_dt"))
        pill_class = "up" if ret >= 0 else "down"
        slot = f"{8 + idx:02d}.00"
        rows.append(
            f"""
            <div class="overview-item">
              <span class="overview-time">{slot}</span>
              <span class="overview-dot"></span>
              <div class="overview-item-card">
                <div class="overview-item-main">
                  <p class="overview-item-ticker">{ticker}</p>
                  <p class="overview-item-note">{start_dt} to {end_dt}</p>
                </div>
                <span class="overview-pill {pill_class}">{ret:+.2f}%</span>
              </div>
            </div>
            """
        )

    if not rows:
        rows.append(
            """
            <div class="overview-item">
              <span class="overview-time">--.--</span>
              <span class="overview-dot"></span>
              <div class="overview-item-card">
                <div class="overview-item-main">
                  <p class="overview-item-ticker">No ranking data</p>
                  <p class="overview-item-note">Backfill daily data to populate this queue.</p>
                </div>
                <span class="overview-pill down">n/a</span>
              </div>
            </div>
            """
        )

    st.markdown(
        f"""
        <article class="overview-panel">
          <div class="overview-panel-head">
            <h3 class="overview-panel-title">Today Appointments</h3>
            <span class="overview-panel-chip">⌚</span>
          </div>
          <div class="overview-panel-body">
            <p class="overview-section-label">Attended Appointments</p>
            <div class="overview-progress-wrap">
              <div class="overview-progress-card">
                <div class="overview-progress-head">
                  <p class="overview-progress-title">7D Positive</p>
                  <p class="overview-progress-value">{pos_7}/{total_7}</p>
                </div>
                <div class="overview-progress-track"><div class="overview-progress-fill" style="width:{pct_7}%;"></div></div>
              </div>
              <div class="overview-progress-card">
                <div class="overview-progress-head">
                  <p class="overview-progress-title">30D Positive</p>
                  <p class="overview-progress-value">{pos_30}/{total_30}</p>
                </div>
                <div class="overview-progress-track"><div class="overview-progress-fill" style="width:{pct_30}%;"></div></div>
              </div>
            </div>
            <div class="overview-list-head">
              <p class="overview-list-title">Appointment</p>
              <span class="overview-list-count">{len(rows)}</span>
            </div>
            <div class="overview-timeline">
              {"".join(rows)}
            </div>
          </div>
        </article>
        """,
        unsafe_allow_html=True,
    )


def render_overview_leader_panel(rank_7d: pd.DataFrame, rank_30d: pd.DataFrame) -> None:
    if rank_30d.empty and rank_7d.empty:
        st.info("No ranking data available for the current selection.")
        return

    leader_row = rank_30d.iloc[0] if not rank_30d.empty else rank_7d.iloc[0]
    ticker = str(leader_row.get("ticker", "n/a"))
    return_30 = _safe_float(leader_row.get("return_pct"), 0.0)
    start_dt = _fmt_short_date(leader_row.get("start_dt"))
    latest_dt = _fmt_short_date(leader_row.get("latest_dt"))

    return_7 = None
    if not rank_7d.empty:
        matches = rank_7d.loc[rank_7d["ticker"] == ticker, "return_pct"]
        if not matches.empty:
            return_7 = _safe_float(matches.iloc[0], 0.0)

    total_30 = len(rank_30d)
    pos_30 = int((rank_30d["return_pct"] > 0).sum()) if total_30 else 0
    breadth_30 = f"{pos_30}/{total_30}" if total_30 else "n/a"

    top_peers = rank_30d.head(3) if not rank_30d.empty else rank_7d.head(3)
    peer_rows = []
    for _, row in top_peers.iterrows():
        peer_rows.append(f"{html.escape(str(row['ticker']))} ({_safe_float(row['return_pct']):+.2f}%)")
    peers = ", ".join(peer_rows) if peer_rows else "n/a"

    signal_7_text = "n/a" if return_7 is None else f"{return_7:+.2f}%"
    trend_class = "up" if return_30 >= 0 else "down"
    trend_label = "Uptrend" if return_30 >= 0 else "Downtrend"

    st.markdown(
        f"""
        <article class="overview-panel">
          <div class="overview-panel-head">
            <h3 class="overview-panel-title">On Going Appointments</h3>
            <span class="overview-panel-chip">◎</span>
          </div>
          <div class="overview-panel-body">
            <div class="overview-leader-top">
              <div style="display:flex;align-items:center;gap:0.72rem;">
                <span class="overview-avatar"></span>
                <div>
                  <p class="overview-leader-name">{html.escape(ticker)}</p>
                  <p class="overview-leader-sub">Primary ranked instrument • {trend_label}</p>
                </div>
              </div>
              <span class="overview-pill {trend_class}">{return_30:+.2f}%</span>
            </div>
            <div class="overview-stats-grid">
              <div>
                <p class="overview-stat-label">Referring Doctor</p>
                <p class="overview-stat-value">7D Return</p>
                <p class="overview-stat-value">{signal_7_text}</p>
              </div>
              <div>
                <p class="overview-stat-label">Assigned Doctor</p>
                <p class="overview-stat-value">30D Breadth</p>
                <p class="overview-stat-value">{breadth_30}</p>
              </div>
              <div>
                <p class="overview-stat-label">Started</p>
                <p class="overview-stat-value">{html.escape(start_dt)}</p>
              </div>
              <div>
                <p class="overview-stat-label">Latest Print</p>
                <p class="overview-stat-value">{html.escape(latest_dt)}</p>
              </div>
            </div>
            <p class="overview-note">
              Peer set: {html.escape(peers)}.
            </p>
          </div>
        </article>
        """,
        unsafe_allow_html=True,
    )


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


def day_window_utc(trade_day: date, tz_name: str) -> tuple[datetime, datetime]:
    tz = ZoneInfo(tz_name)
    local_start = datetime.combine(trade_day, datetime.min.time(), tzinfo=tz)
    local_end = local_start + timedelta(days=1)
    return local_start.astimezone(timezone.utc), local_end.astimezone(timezone.utc)


def fetch_intraday_top_performers(
    tickers: Sequence[str],
    day_start_utc: datetime,
    day_end_utc: datetime,
    top_n: int,
) -> pd.DataFrame:
    sql = """
    WITH day_px AS (
      SELECT
        p.ticker,
        p.ts,
        p.close::float AS close,
        COALESCE(p.volume, 0)::float AS volume
      FROM prices_1m p
      WHERE p.ticker = ANY(%s)
        AND p.ts >= %s
        AND p.ts < %s
        AND p.close IS NOT NULL
    ),
    first_px AS (
      SELECT DISTINCT ON (ticker)
        ticker,
        ts AS first_ts,
        close AS first_close
      FROM day_px
      ORDER BY ticker, ts ASC
    ),
    last_px AS (
      SELECT DISTINCT ON (ticker)
        ticker,
        ts AS last_ts,
        close AS last_close
      FROM day_px
      ORDER BY ticker, ts DESC
    ),
    agg AS (
      SELECT
        ticker,
        COUNT(*) AS bars,
        SUM(volume)::float AS volume_sum
      FROM day_px
      GROUP BY ticker
    )
    SELECT
      l.ticker,
      f.first_ts,
      l.last_ts,
      f.first_close,
      l.last_close,
      CASE
        WHEN f.first_close IS NULL OR f.first_close = 0 THEN NULL
        ELSE ((l.last_close / f.first_close) - 1.0) * 100.0
      END AS return_pct,
      (l.last_close - f.first_close) AS change_abs,
      a.bars,
      a.volume_sum
    FROM first_px f
    JOIN last_px l USING (ticker)
    JOIN agg a USING (ticker)
    WHERE f.first_close IS NOT NULL
    ORDER BY return_pct DESC NULLS LAST, l.ticker
    LIMIT %s;
    """
    return qdf(sql, (list(tickers), day_start_utc, day_end_utc, top_n))


def fetch_intraday_normalized_series(
    tickers: Sequence[str],
    day_start_utc: datetime,
    day_end_utc: datetime,
) -> pd.DataFrame:
    sql = """
    SELECT
      p.ticker,
      p.ts,
      p.close::float AS close
    FROM prices_1m p
    WHERE p.ticker = ANY(%s)
      AND p.ts >= %s
      AND p.ts < %s
      AND p.close IS NOT NULL
    ORDER BY p.ticker, p.ts;
    """
    out = qdf(sql, (list(tickers), day_start_utc, day_end_utc))
    if out.empty:
        return out

    out["ts"] = pd.to_datetime(out["ts"], utc=True)
    out = out.sort_values(["ticker", "ts"]).reset_index(drop=True)
    out["base_close"] = out.groupby("ticker")["close"].transform("first")
    out["norm_close"] = out["close"] / out["base_close"]
    out.loc[(out["base_close"].isna()) | (out["base_close"] == 0), "norm_close"] = pd.NA
    return out[["ticker", "ts", "close", "norm_close"]]


def render_intraday_performer_page() -> None:
    _, _, universe_tickers = render_classification_filters("intraday")
    st.caption("Intraday rankings use only minute data from `prices_1m`.")

    if not universe_tickers:
        st.info("No tickers available for this filter selection.")
        st.stop()

    t1, t2, t3 = st.columns(3)
    with t1:
        tz_name = st.selectbox(
            "Trading timezone",
            ["America/New_York", "UTC", "America/Los_Angeles"],
            index=0,
            key="intraday_tz",
        )
    default_trade_day = datetime.now(ZoneInfo(tz_name)).date()
    with t2:
        trade_day = st.date_input("Trading day", value=default_trade_day, key="intraday_trade_day")
    with t3:
        top_n = st.slider("Top N", min_value=3, max_value=30, value=5, step=1, key="intraday_top_n")

    day_start_utc, day_end_utc = day_window_utc(trade_day, tz_name)
    rank_df = fetch_intraday_top_performers(universe_tickers, day_start_utc, day_end_utc, top_n)
    if rank_df.empty:
        st.warning(
            "No minute-level data found for this window. "
            "Run the intraday updater and confirm `prices_1m` has rows for this day."
        )
        return

    rank_df["first_ts"] = pd.to_datetime(rank_df["first_ts"], utc=True).dt.tz_convert(tz_name)
    rank_df["last_ts"] = pd.to_datetime(rank_df["last_ts"], utc=True).dt.tz_convert(tz_name)
    leader = rank_df.iloc[0]

    m1, m2, m3, m4 = st.columns(4)
    with m1:
        st.metric("Leader", leader["ticker"], f"{leader['return_pct']:.2f}%")
    with m2:
        st.metric("Avg Return", f"{rank_df['return_pct'].mean():.2f}%")
    with m3:
        st.metric("Constituents", f"{len(rank_df)}")
    with m4:
        latest_print = rank_df["last_ts"].max()
        st.metric("Latest Print", latest_print.strftime("%H:%M"))

    st.caption(
        f"Session window ({tz_name}): {trade_day.isoformat()} 00:00 -> "
        f"{(trade_day + timedelta(days=1)).isoformat()} 00:00"
    )
    st.dataframe(rank_df, use_container_width=True)

    bar_df = rank_df.sort_values("return_pct", ascending=True).copy()
    bar_fig = px.bar(
        bar_df,
        x="return_pct",
        y="ticker",
        orientation="h",
        title=f"Intraday return ranking ({trade_day.isoformat()})",
        labels={"return_pct": "Return %", "ticker": ""},
        color="return_pct",
        color_continuous_scale=["#86c5da", "#1b6ca8", "#f18f01"],
    )
    bar_fig.update_layout(coloraxis_showscale=False)
    st.plotly_chart(style_figure(bar_fig), use_container_width=True)

    top_tickers = rank_df["ticker"].tolist()
    intraday_df = fetch_intraday_normalized_series(top_tickers, day_start_utc, day_end_utc)
    if intraday_df.empty:
        st.warning("No minute series found for ranked tickers in this window.")
        return

    intraday_df["ts_local"] = pd.to_datetime(intraday_df["ts"], utc=True).dt.tz_convert(tz_name)
    intraday_fig = px.line(
        intraday_df,
        x="ts_local",
        y="norm_close",
        color="ticker",
        title=f"Top {top_n} intraday normalized performance ({trade_day.isoformat()})",
    )
    st.plotly_chart(style_figure(intraday_fig), use_container_width=True)


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
    deployment_values: Sequence[float] | None = None,
    progress_callback: Callable[[int, int], None] | None = None,
) -> pd.DataFrame:
    rows: list[dict] = []
    deploy_vals = [None] if not deployment_values else [float(v) for v in deployment_values]
    combos = list(
        product(
            buy_threshold_values,
            buy_window_values,
            sell_threshold_values,
            sell_window_values,
            deploy_vals,
        )
    )
    total = len(combos)

    for idx, (buy_th, buy_win, sell_th, sell_win, deploy_unit) in enumerate(combos, start=1):
        local_buy_units = buy_unit_by_ticker
        if deploy_unit is not None:
            local_buy_units = {t: float(deploy_unit) for t in buy_unit_by_ticker.keys()}

        trades_df, _, equity_df = run_threshold_simulation(
            price_panel=price_panel,
            buy_unit_by_ticker=local_buy_units,
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
                "deployment_per_trade_usd": deploy_unit,
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
st.title("Overview")
st.markdown(
    """
    <div class="hero">
      <div class="hero-head">
        <span class="hero-icon"></span>
        <p class="hero-title">Overview</p>
      </div>
      <p class="hero-subtitle">
        Sector intelligence, intraday leadership, and strategy simulation in one operational dashboard.
      </p>
    </div>
    """,
    unsafe_allow_html=True,
)
with st.sidebar:
    st.markdown(
        """
        <div class="sidebar-brand">
          <span class="sidebar-badge">✓</span>
          <span class="sidebar-brand-text">Shield</span>
        </div>
        <p class="sidebar-section">Main Menu</p>
        """,
        unsafe_allow_html=True,
    )
    nav_choice = st.radio(
        "Navigation",
        ["Overview", "Top Performers", "Intraday Movers", "Strategy Simulator"],
        index=0,
        key="sidebar_nav",
        label_visibility="collapsed",
    )
    st.markdown(
        """
        <p class="sidebar-section">Other Menu</p>
        <div class="sidebar-item muted">Schedules</div>
        <div class="sidebar-item muted">Payment</div>
        <div class="sidebar-item muted">Product & Stock</div>
        <p class="sidebar-section">Help & Settings</p>
        <div class="sidebar-item muted">Help Center</div>
        <div class="sidebar-item muted">Settings</div>
        <div class="sidebar-item muted">Report</div>
        """,
        unsafe_allow_html=True,
    )

page = {
    "Overview": "Explorer",
    "Top Performers": "Top Performers",
    "Intraday Movers": "Intraday Performers",
    "Strategy Simulator": "Strategy Simulator",
}[nav_choice]

if page == "Explorer":
    sector_choice, subsector_choice, tickers = render_classification_filters("explorer")
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

    rank_limit = max(5, len(selected_tickers))
    rank_7 = fetch_top_performers(selected_tickers, lookback_days=7, top_n=rank_limit)
    rank_30 = fetch_top_performers(selected_tickers, lookback_days=30, top_n=rank_limit)

    selected_total = len(selected_tickers)
    pos_7 = int((rank_7["return_pct"] > 0).sum()) if not rank_7.empty else 0
    pos_30 = int((rank_30["return_pct"] > 0).sum()) if not rank_30.empty else 0
    latest_dt = df["dt"].max()
    latest_coverage = int(df.loc[df["dt"] == latest_dt, "ticker"].nunique())
    coverage_pct = (latest_coverage / selected_total) * 100.0 if selected_total else 0.0

    coverage_spark = df.groupby("dt", as_index=False)["ticker"].nunique()["ticker"].tail(6).tolist()
    rank7_spark = rank_7["return_pct"].head(6).tolist() if not rank_7.empty else []
    rank30_spark = rank_30["return_pct"].head(6).tolist() if not rank_30.empty else []
    universe_spark = [len(tickers), selected_total, latest_coverage]

    render_overview_stat_cards(
        [
            {
                "title": "Appointments",
                "value": f"{selected_total}",
                "subtitle": f"Universe from {sector_choice}/{subsector_choice}",
                "icon": "◧",
                "accent": "#355f92",
                "spark": universe_spark,
            },
            {
                "title": "New Patients",
                "value": f"{pos_7}",
                "subtitle": "7-day positive tickers",
                "icon": "◍",
                "accent": "#df6323",
                "spark": rank7_spark,
            },
            {
                "title": "Follow-Up Patients",
                "value": f"{pos_30}",
                "subtitle": "30-day positive tickers",
                "icon": "◌",
                "accent": "#c79a2f",
                "spark": rank30_spark,
            },
            {
                "title": "Review Report",
                "value": f"{coverage_pct:.0f}%",
                "subtitle": "Latest date coverage",
                "icon": "▣",
                "accent": "#3a8a55",
                "spark": coverage_spark,
            },
        ]
    )

    left_col, right_col = st.columns([1.05, 1.4], gap="medium")
    with left_col:
        render_overview_queue_panel(rank_7, rank_30, max_items=5)
    with right_col:
        render_overview_leader_panel(rank_7, rank_30)

    perf_tab, summary_tab, builder_tab = st.tabs(
        ["Normalized Performance", "Return Summary", "Custom Basket Builder"]
    )

    with perf_tab:
        st.subheader("Normalized performance (starts at 1.0)")
        fig = px.line(df, x="dt", y="norm_close", color="ticker")
        st.plotly_chart(style_figure(fig), use_container_width=True)

    with summary_tab:
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

    with builder_tab:
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
elif page == "Intraday Performers":
    render_intraday_performer_page()
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
    st.caption(
        f"Leverage: {'ON' if allow_leverage else 'OFF'} | "
        f"Cash yield: {annual_cash_yield_pct:.2f}% | Borrow rate: {annual_borrow_rate_pct:.2f}%"
    )
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

    st.markdown("**Position sizing in grid**")
    grid_deployment_mode = st.radio(
        "Grid deployment sizing",
        ["Use current simulator sizing", "Sweep deployment per trade (USD)"],
        horizontal=True,
        key="sim_grid_deployment_mode",
    )
    grid_deployment_values: list[float] | None = None
    if grid_deployment_mode == "Sweep deployment per trade (USD)":
        gd1, gd2, gd3 = st.columns(3)
        with gd1:
            dep_min = st.number_input("Deploy min (USD)", min_value=0.0, max_value=1_000_000.0, value=500.0, step=100.0)
        with gd2:
            dep_max = st.number_input("Deploy max (USD)", min_value=0.0, max_value=1_000_000.0, value=3000.0, step=100.0)
        with gd3:
            dep_step = st.number_input("Deploy step (USD)", min_value=50.0, max_value=100_000.0, value=500.0, step=50.0)
        if dep_min > dep_max:
            st.error("Deploy min must be <= deploy max.")
            st.stop()
        grid_deployment_values = build_float_grid(dep_min, dep_max, dep_step)

    if buy_th_min > buy_th_max or buy_win_min > buy_win_max or sell_th_min > sell_th_max or sell_win_min > sell_win_max:
        st.error("Each min value must be <= max value for grid search.")
        st.stop()

    buy_th_values = build_float_grid(buy_th_min, buy_th_max, buy_th_step)
    buy_win_values = build_int_grid(int(buy_win_min), int(buy_win_max), int(buy_win_step))
    sell_th_values = build_float_grid(sell_th_min, sell_th_max, sell_th_step)
    sell_win_values = build_int_grid(int(sell_win_min), int(sell_win_max), int(sell_win_step))

    dep_count = 1 if not grid_deployment_values else len(grid_deployment_values)
    combo_count = len(buy_th_values) * len(buy_win_values) * len(sell_th_values) * len(sell_win_values) * dep_count
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
                deployment_values=grid_deployment_values,
                progress_callback=on_progress,
            )
        progress_text.caption(f"Grid progress: {combo_count} / {combo_count}")
        progress_bar.progress(100)

        if grid_df.empty:
            st.warning("Grid search returned no results.")
        else:
            best = grid_df.iloc[0]
            b1, b2, b3, b4, b5, b6 = st.columns(6)
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
            with b6:
                best_dep = best.get("deployment_per_trade_usd")
                dep_label = "current sizing" if pd.isna(best_dep) else f"${float(best_dep):,.0f}"
                st.metric("Best Deployment", dep_label)

            st.dataframe(grid_df.head(30), use_container_width=True)
