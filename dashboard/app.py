"""
Streamlit dashboard for the Stock Analytics Data Service.

Visual proof of the dual-engine FastAPI layer: search via ElasticSearch,
analytics via ClickHouse. Five views + engine latency comparison.

Prerequisites:
    docker compose up -d
    uvicorn src.api.main:app --reload --port 8000

Usage:
    streamlit run dashboard/app.py
"""

from __future__ import annotations

import sys
from pathlib import Path

import pandas as pd
import streamlit as st

# Allow imports when launched from repo root
_ROOT = Path(__file__).resolve().parent.parent
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

from dashboard.api_client import AnalyticsApiClient, DEFAULT_API_BASE

st.set_page_config(
    page_title="Stock Analytics Dashboard",
    page_icon="📈",
    layout="wide",
)

# Simulator tickers (Week 3 producer + company_metadata) + Finnhub live tickers
TICKERS = [
    "NVDA", "AAPL", "MSFT", "GOOG", "AMZN", "META", "TSLA",
    "JPM", "GS", "MS", "BAC", "BRK-B", "AXP",
    "UNH", "ALL", "PGR", "TRV",
    "SNOW", "CRM", "UBER", "NFLX",
    "V", "MA", "PYPL", "XYZ",
    # Finnhub live tickers
    "ABBV", "AVGO", "BRK.B", "COST", "CVX", "GOOGL",
    "HD", "JNJ", "KO", "LLY", "MRK", "PEP",
    "PG", "TMO", "WMT", "XOM",
]


def _api() -> AnalyticsApiClient:
    base = st.session_state.get("api_base", DEFAULT_API_BASE)
    return AnalyticsApiClient(base_url=base)


def _sidebar() -> None:
    st.sidebar.title("Stock Analytics")
    st.session_state["api_base"] = st.sidebar.text_input(
        "API base URL",
        value=st.session_state.get("api_base", DEFAULT_API_BASE),
    )
    st.sidebar.caption("FastAPI service (analytics → ClickHouse, search → ES)")

    health = _api().health()
    if health.error:
        st.sidebar.error(f"API unreachable: {health.error}")
        st.sidebar.info("Start: `uvicorn src.api.main:app --port 8000`")
    else:
        status = (health.data or {}).get("status", "unknown")
        st.sidebar.metric("Service status", status)
        ch = (health.data or {}).get("clickhouse", {})
        es = (health.data or {}).get("elasticsearch", {})
        st.sidebar.caption(
            f"CH: {ch.get('status', '?')} ({ch.get('latency_ms', '?')} ms) · "
            f"ES: {es.get('status', '?')} ({es.get('latency_ms', '?')} ms)"
        )


def page_overview() -> None:
    st.header("Market overview")
    st.caption("Single ClickHouse-backed call — `/api/v1/analytics/market/summary`")

    resp = _api().market_summary()
    if resp.error:
        st.error(resp.error)
        return

    st.metric("API latency", f"{resp.latency_ms:.0f} ms")
    data = resp.data or {}
    col1, col2, col3 = st.columns(3)
    col1.metric("Total trades", f"{data.get('total_trades', 0):,}")
    col2.metric("Total volume", f"{data.get('total_volume', 0):,}")
    col3.metric("Tickers", data.get("total_tickers", 0))

    movers = data.get("top_movers") or []
    if movers:
        st.subheader("Top movers (from summary)")
        st.dataframe(pd.DataFrame(movers), use_container_width=True, hide_index=True)

    sectors = data.get("sector_performance") or []
    if sectors:
        st.subheader("Sector snapshot")
        df = pd.DataFrame(sectors)
        st.bar_chart(df.set_index("sector")["total_notional"])


def page_ticker_search() -> None:
    st.header("Ticker search")
    st.caption("ElasticSearch autocomplete — `/api/v1/search/autocomplete`")

    query = st.text_input("Type a ticker or company prefix", value="Goo")
    if len(query) < 1:
        st.info("Enter at least one character.")
        return

    resp = _api().autocomplete(query, limit=10)
    if resp.error:
        st.error(resp.error)
        return

    st.metric("API latency", f"{resp.latency_ms:.0f} ms")
    results = resp.data or []
    if not results:
        st.warning("No suggestions — is ElasticSearch loaded?")
        return

    st.dataframe(pd.DataFrame(results), use_container_width=True, hide_index=True)


def page_vwap() -> None:
    st.header("VWAP chart")
    st.caption("ClickHouse OLAP — `/api/v1/analytics/vwap/{ticker}`")

    col1, col2 = st.columns(2)
    ticker = col1.selectbox("Ticker", TICKERS)
    granularity = col2.selectbox("Granularity", ["1min", "5min", "1h", "1d"], index=1)

    resp = _api().vwap(ticker, granularity=granularity, limit=200)
    if resp.error:
        st.error(resp.error)
        return

    st.metric("API latency", f"{resp.latency_ms:.0f} ms")
    points = (resp.data or {}).get("data") or []
    if not points:
        st.warning("No VWAP data for this ticker.")
        return

    df = pd.DataFrame(points)
    df["window_start"] = pd.to_datetime(df["window_start"])
    df = df.sort_values("window_start")
    chart_df = df.set_index("window_start")[["vwap"]]
    st.line_chart(chart_df)
    with st.expander("Raw data"):
        st.dataframe(df, use_container_width=True, hide_index=True)


def page_sectors() -> None:
    st.header("Sector comparison")
    st.caption("ClickHouse JOIN aggregation — `/api/v1/analytics/sectors/performance`")

    resp = _api().sector_performance()
    if resp.error:
        st.error(resp.error)
        return

    st.metric("API latency", f"{resp.latency_ms:.0f} ms")
    rows = resp.data or []
    if not rows:
        st.warning("No sector data.")
        return

    df = pd.DataFrame(rows)
    st.bar_chart(df.set_index("sector")["total_notional"])
    st.dataframe(df, use_container_width=True, hide_index=True)


def page_anomalies() -> None:
    st.header("Anomaly alerts")
    st.caption("ClickHouse surveillance — `/api/v1/analytics/anomalies`")

    col1, col2, col3 = st.columns(3)
    anomaly_ticker = col1.selectbox(
        "Filter by ticker",
        [None] + TICKERS,
        key="anomaly_ticker",
    )
    min_dev = col2.slider("Min deviation (σ)", 1.0, 5.0, 2.0, 0.5)
    limit = col3.number_input("Limit", 10, 200, 50, 10)

    resp = _api().anomalies(
        ticker=anomaly_ticker,
        min_deviation=min_dev,
        limit=int(limit),
    )
    if resp.error:
        st.error(resp.error)
        return

    st.metric("API latency", f"{resp.latency_ms:.0f} ms")
    rows = resp.data or []
    if not rows:
        st.success("No anomalies above threshold.")
        return

    st.dataframe(pd.DataFrame(rows), use_container_width=True, hide_index=True)


def page_engine_toggle() -> None:
    st.header("Engine toggle")
    st.caption(
        "Same ticker, two engines: ClickHouse structured browse vs ElasticSearch search. "
        "Demonstrates routing and client-side latency — not identical SQL."
    )

    ticker = st.selectbox("Ticker", TICKERS, index=0)
    limit = st.slider("Rows to fetch", 5, 50, 20)

    if st.button("Run comparison", type="primary"):
        col_ch, col_es = st.columns(2)

        with col_ch:
            st.subheader("ClickHouse path")
            st.code("GET /api/v1/analytics/trades?ticker=…")
            ch_resp = _api().browse_trades_ch(ticker, limit=limit)
            if ch_resp.error:
                st.error(ch_resp.error)
            else:
                st.metric("Latency", f"{ch_resp.latency_ms:.0f} ms")
                trades = (ch_resp.data or {}).get("trades") or []
                st.write(f"**{len(trades)}** trades returned")
                if trades:
                    st.dataframe(pd.DataFrame(trades), use_container_width=True, hide_index=True)

        with col_es:
            st.subheader("ElasticSearch path")
            st.code("GET /api/v1/search/trades?q=…&ticker=…")
            es_resp = _api().search_trades_es(ticker, limit=limit)
            if es_resp.error:
                st.error(es_resp.error)
            else:
                st.metric("Latency", f"{es_resp.latency_ms:.0f} ms")
                payload = es_resp.data or {}
                results = payload.get("results") or []
                st.write(f"**{payload.get('total_hits', 0)}** total hits · **{len(results)}** shown")
                if results:
                    st.dataframe(pd.DataFrame(results), use_container_width=True, hide_index=True)

        if not ch_resp.error and not es_resp.error:
            faster = "ClickHouse" if ch_resp.latency_ms <= es_resp.latency_ms else "ElasticSearch"
            st.info(
                f"Faster for this pattern: **{faster}** "
                f"({min(ch_resp.latency_ms, es_resp.latency_ms):.0f} ms vs "
                f"{max(ch_resp.latency_ms, es_resp.latency_ms):.0f} ms). "
                "OLAP vs search — different strengths."
            )

    st.divider()
    st.subheader("VWAP (ClickHouse only)")
    st.caption("ElasticSearch has no native VWAP endpoint — analytics stay on ClickHouse.")
    if st.button("Fetch VWAP on ClickHouse"):
        vwap_resp = _api().vwap(ticker, limit=30)
        if vwap_resp.error:
            st.error(vwap_resp.error)
        else:
            st.metric("CH VWAP latency", f"{vwap_resp.latency_ms:.0f} ms")
            points = (vwap_resp.data or {}).get("data") or []
            st.write(f"{len(points)} buckets")


def main() -> None:
    _sidebar()

    pages = {
        "Overview": page_overview,
        "Ticker search": page_ticker_search,
        "VWAP chart": page_vwap,
        "Sector comparison": page_sectors,
        "Anomaly alerts": page_anomalies,
        "Engine toggle": page_engine_toggle,
    }

    choice = st.sidebar.radio("View", list(pages.keys()))
    pages[choice]()


if __name__ == "__main__":
    main()
