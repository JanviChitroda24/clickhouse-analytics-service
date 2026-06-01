"""
Cross-engine data quality checks for ClickHouse, ElasticSearch, and SQL Server.

Runs seven consistency validations, writes docs/data_quality_report.md, and exits
with code 1 if any check fails (for orchestration gates that block downstream jobs).

Usage:
    python -m src.data_quality_checks
"""

from __future__ import annotations

import logging
import sys
from datetime import datetime, timezone
from pathlib import Path

from src.clickhouse_client import execute_query as ch_query
from src.config import get_settings
from src.es_client import get_es_client
from src.sqlserver_client import execute_query as sql_query

logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")
logger = logging.getLogger(__name__)

_REPO_ROOT = Path(__file__).resolve().parent.parent
_REPORT_PATH = _REPO_ROOT / "docs" / "data_quality_report.md"

# ES mapping must stay aligned with src/es_index_setup.py TRADE_INDEX_MAPPING
_EXPECTED_ES_FIELDS: dict[str, str] = {
    "trade_id": "keyword",
    "ticker": "keyword",
    "ticker_text": "text",
    "price": "float",
    "quantity": "integer",
    "trade_time": "date",
    "side": "keyword",
    "trade_type": "keyword",
    "source": "keyword",
    "sector": "keyword",
    "company_name": "text",
    "suggest": "completion",
    "bid_price": "float",
    "ask_price": "float",
}

# Tolerance for MV vs raw VWAP (floating-point aggregation across minute buckets)
_VWAP_TOLERANCE = 0.01
# Max lag between latest CH and ES trade_time when both engines are ingesting live
_FRESHNESS_TOLERANCE_SECONDS = 300
# Allow ES to trail CH slightly when Kafka consumer is behind bulk-loaded baseline
_ROW_COUNT_ES_LAG_TOLERANCE = 100


def _es_index() -> str:
    return get_settings().es_index_trades


def check_cross_engine_row_count() -> tuple[bool, str]:
    """
    Compare row/document counts across all three engines.

    Catches bulk-load drops, consumer failures, or engines that stopped receiving
    Kafka traffic (SQL Server has no live consumer in this project).
    """
    ch_count = int(ch_query("SELECT count() FROM stock_analytics.raw_trades")[0][0])

    es_client = get_es_client()
    es_client.indices.refresh(index=_es_index())
    es_count = int(es_client.count(index=_es_index())["count"])

    sql_count = int(
        sql_query("SELECT COUNT(*) FROM stock_analytics.dbo.raw_trades")[0][0]
    )

    msg = f"CH={ch_count:,}, ES={es_count:,}, SQL={sql_count:,}"
    if ch_count == es_count == sql_count:
        return True, msg

    # ES may lag CH by a few documents during live ingestion; SQL should match CH snapshot
    ch_es_diff = abs(ch_count - es_count)
    if ch_es_diff < _ROW_COUNT_ES_LAG_TOLERANCE and ch_count == sql_count:
        return True, f"{msg} (ES within {_ROW_COUNT_ES_LAG_TOLERANCE} of CH — acceptable lag)"

    return False, msg


def check_mv_consistency() -> tuple[bool, str]:
    """
    Verify pre-aggregated VWAP in mv_realtime_vwap matches a fresh scan of raw_trades.

    Detects MV definition bugs or missing backfill after the MV was created post-load.
    """
    mv_result = ch_query(
        """
        SELECT sumMerge(price_volume_sum) / sumMerge(volume_sum) AS vwap
        FROM stock_analytics.mv_realtime_vwap
        WHERE ticker = 'AAPL'
        """
    )
    mv_vwap = mv_result[0][0] if mv_result else None

    raw_result = ch_query(
        """
        SELECT sum(price * quantity) / sum(quantity) AS vwap
        FROM stock_analytics.raw_trades
        WHERE ticker = 'AAPL'
        """
    )
    raw_vwap = raw_result[0][0] if raw_result else None

    if mv_vwap is None or raw_vwap is None:
        return False, f"MV VWAP={mv_vwap}, Raw VWAP={raw_vwap} — null detected"

    diff = abs(float(mv_vwap) - float(raw_vwap))
    passed = diff < _VWAP_TOLERANCE
    return passed, f"MV VWAP={float(mv_vwap):.4f}, Raw VWAP={float(raw_vwap):.4f}, diff={diff:.6f}"


def check_es_index_health() -> tuple[bool, str]:
    """
    Ensure the ElasticSearch cluster is not in red state (primary shards missing).

    Yellow is acceptable on a single-node dev cluster (unassigned replicas).
    """
    health = get_es_client().cluster.health()
    status = health["status"]
    passed = status in ("green", "yellow")
    return passed, f"Cluster status: {status}"


def _parse_es_trade_time(value: str | datetime) -> datetime | None:
    """Normalize ES trade_time to naive datetime for comparison with ClickHouse."""
    if isinstance(value, datetime):
        dt = value
    else:
        normalized = str(value).replace("Z", "").replace("+00:00", "")
        try:
            dt = datetime.fromisoformat(normalized)
        except ValueError:
            return None
    if dt.tzinfo is not None:
        dt = dt.replace(tzinfo=None)
    return dt


def check_freshness() -> tuple[bool, str]:
    """
    Compare latest trade_time in ClickHouse vs ElasticSearch.

    Large gaps usually mean a stalled Kafka consumer or ClickHouse Kafka engine issue.
    """
    ch_result = ch_query("SELECT max(trade_time) FROM stock_analytics.raw_trades")
    ch_latest = ch_result[0][0]

    es_result = get_es_client().search(
        index=_es_index(),
        size=1,
        sort=[{"trade_time": {"order": "desc"}}],
        source=["trade_time"],
    )
    hits = es_result["hits"]["hits"]
    if not hits:
        return False, "ES index has no documents — cannot compare freshness"

    es_latest = _parse_es_trade_time(hits[0]["_source"]["trade_time"])
    if ch_latest is None or es_latest is None:
        return False, f"CH latest={ch_latest}, ES latest={es_latest} — null or unparseable"

    if hasattr(ch_latest, "tzinfo") and ch_latest.tzinfo is not None:
        ch_latest = ch_latest.replace(tzinfo=None)

    diff_seconds = abs((ch_latest - es_latest).total_seconds())
    passed = diff_seconds < _FRESHNESS_TOLERANCE_SECONDS
    return (
        passed,
        f"CH latest={ch_latest}, ES latest={es_latest}, diff={diff_seconds:.0f}s",
    )


def check_schema_drift() -> tuple[bool, str]:
    """
    Confirm the ES index still exposes all fields the API and bulk loader expect.

    Mapping type changes in ES are irreversible without reindexing — catch drift early.
    """
    mapping = get_es_client().indices.get_mapping(index=_es_index())
    properties = mapping[_es_index()]["mappings"]["properties"]

    missing: list[str] = []
    wrong_type: list[str] = []

    for field, expected_type in _EXPECTED_ES_FIELDS.items():
        if field not in properties:
            missing.append(field)
            continue
        actual = properties[field].get("type", "unknown")
        if actual != expected_type:
            wrong_type.append(f"{field}: expected {expected_type}, got {actual}")

    if missing or wrong_type:
        issues: list[str] = []
        if missing:
            issues.append(f"missing: {missing}")
        if wrong_type:
            issues.append(f"wrong type: {wrong_type}")
        return False, "; ".join(issues)

    return True, f"All {len(_EXPECTED_ES_FIELDS)} fields present with correct types"


def check_null_validation() -> tuple[bool, str]:
    """Validate critical ClickHouse columns — nulls or empty tickers break analytics and APIs."""
    null_price, null_qty, null_ticker, null_time = ch_query(
        """
        SELECT
            countIf(price IS NULL OR price = 0) AS null_price,
            countIf(quantity IS NULL OR quantity = 0) AS null_quantity,
            countIf(ticker IS NULL OR ticker = '') AS null_ticker,
            countIf(trade_time IS NULL) AS null_time
        FROM stock_analytics.raw_trades
        """
    )[0]

    total_nulls = null_price + null_qty + null_ticker + null_time
    if total_nulls > 0:
        return (
            False,
            f"null_price={null_price}, null_qty={null_qty}, "
            f"null_ticker={null_ticker}, null_time={null_time}",
        )

    return True, "No nulls in critical fields (price, quantity, ticker, trade_time)"


def check_range_validation() -> tuple[bool, str]:
    """
    Ensure prices sit within realistic bounds for the 25-ticker simulated universe.

    Catches parser bugs and corrupt values that would skew VWAP and anomaly detection.
    """
    below, above, min_p, max_p = ch_query(
        """
        SELECT
            countIf(price < 1) AS below_min,
            countIf(price > 2000) AS above_max,
            min(price) AS min_price,
            max(price) AS max_price
        FROM stock_analytics.raw_trades
        """
    )[0]

    total_violations = below + above
    if total_violations > 0:
        return (
            False,
            f"{total_violations} violations: {below} below $1, {above} above $2000 "
            f"(range: ${float(min_p):.2f}–${float(max_p):.2f})",
        )

    return True, f"All prices in range $1–$2000 (actual: ${float(min_p):.2f}–${float(max_p):.2f})"


ALL_CHECKS: list[tuple[str, object]] = [
    ("Cross-engine row count", check_cross_engine_row_count),
    ("ClickHouse MV consistency", check_mv_consistency),
    ("ES index health", check_es_index_health),
    ("Freshness check (CH vs ES)", check_freshness),
    ("Schema drift detection", check_schema_drift),
    ("Null validation", check_null_validation),
    ("Range validation", check_range_validation),
]


def generate_report(results: list[tuple[str, bool, str]]) -> None:
    """Write pass/fail table and check descriptions to docs/data_quality_report.md."""
    passed_count = sum(1 for _, passed, _ in results if passed)
    total = len(results)
    status = "✅ ALL PASSED" if passed_count == total else f"⚠️ {total - passed_count} FAILED"

    lines = [
        "# Data Quality Report",
        "",
        f"**Generated:** {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')}",
        "",
        f"**Status:** {status} ({passed_count}/{total})",
        "",
        "## Check Results",
        "",
        "| Check | Status | Details |",
        "|-------|--------|---------|",
    ]

    for name, passed, msg in results:
        icon = "✅" if passed else "❌"
        lines.append(f"| {name} | {icon} | {msg} |")

    lines.extend(
        [
            "",
            "## Checks Explained",
            "",
            "1. **Cross-engine row count** — ClickHouse, ES, and SQL Server should have identical row counts",
            "2. **MV consistency** — Materialized view VWAP matches fresh raw_trades calculation",
            "3. **ES index health** — Cluster status green or yellow (not red)",
            "4. **Freshness** — Latest timestamps in CH and ES within 5 minutes of each other",
            "5. **Schema drift** — ES index has all 14 expected fields with correct types",
            "6. **Null validation** — No nulls in price, quantity, ticker, trade_time",
            "7. **Range validation** — All prices between $1 and $2,000",
        ]
    )

    _REPORT_PATH.parent.mkdir(parents=True, exist_ok=True)
    _REPORT_PATH.write_text("\n".join(lines) + "\n", encoding="utf-8")
    logger.info("Report written: %s", _REPORT_PATH)


def main() -> None:
    logger.info("=" * 60)
    logger.info("Data Quality Checks — Cross-Engine Consistency")
    logger.info("=" * 60)

    results: list[tuple[str, bool, str]] = []
    any_failed = False

    for name, check_fn in ALL_CHECKS:
        try:
            passed, msg = check_fn()
        except Exception as exc:
            passed, msg = False, f"ERROR: {exc}"

        icon = "✅" if passed else "❌"
        logger.info("  %s %s: %s", icon, name, msg)

        results.append((name, passed, msg))
        if not passed:
            any_failed = True

    logger.info("")
    generate_report(results)

    passed_count = sum(1 for _, passed, _ in results if passed)
    logger.info("\n%s", "=" * 60)
    logger.info("Results: %d/%d checks passed", passed_count, len(results))

    if any_failed:
        logger.error("Some checks failed — investigate before proceeding")
        sys.exit(1)

    logger.info("All checks passed — data quality verified")
    sys.exit(0)


if __name__ == "__main__":
    main()
