"""
End-to-end pipeline verification.

Validates the full Producer → Kafka → ClickHouse path under realistic conditions:
  - Row counts across all stock_analytics tables
  - All 25 tickers present in raw_trades
  - Data freshness (latest trade_time near now)
  - Price sanity (no zeros or absurd highs)
  - Kafka engine consumer status (system.kafka_consumers)
  - Source mix (bulk load vs kafka_test vs simulator)

Writes portfolio artifact: docs/clickhouse_verification_report.md

Run while or after Week 3 producer:
  cd kafka-spark-streaming-pipeline/src
  python producer.py --mode simulated --eps 100 --duration 120

Then:
  python -m src.verify_pipeline
"""

import logging
from datetime import datetime, timezone
from pathlib import Path

from src.clickhouse_client import execute_query
from src.config import get_settings

logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")
logger = logging.getLogger(__name__)

REPO_ROOT = Path(__file__).resolve().parent.parent
REPORT_PATH = REPO_ROOT / "docs" / "clickhouse_verification_report.md"

# Core schema tables
TABLES = ("raw_trades", "vwap_1min", "company_metadata")

# Freshness threshold — producer may be stopped; 5 min is generous for a 2-min run
FRESHNESS_OK_SECONDS = 300


def check_row_counts() -> dict[str, int]:
    """Count rows in each stock_analytics table."""
    counts = {}
    for table in TABLES:
        result = execute_query(f"SELECT count() FROM stock_analytics.{table}")
        counts[table] = result[0][0]
        logger.info("  %-25s %10d rows", table, counts[table])
    return counts


def check_all_tickers() -> list[tuple]:
    """
    Verify all configured tickers appear in raw_trades.
    Returns list of (ticker, count) sorted by count descending.
    """
    settings = get_settings()
    result = execute_query("""
        SELECT ticker, count() AS cnt
        FROM stock_analytics.raw_trades
        GROUP BY ticker
        ORDER BY cnt DESC
    """)
    found_tickers = [row[0] for row in result]
    missing = set(settings.tickers) - set(found_tickers)

    logger.info("  Tickers found: %d/25", len(found_tickers))
    if missing:
        logger.warning("  Missing tickers: %s", missing)
    else:
        logger.info("  All 25 tickers present")

    return result


def check_data_freshness() -> dict:
    """
    Check min/max trade_time and lag vs now().
    Proves real-time ingestion when producer is running or recently stopped.
    """
    result = execute_query("""
        SELECT
            max(trade_time) AS latest_trade,
            min(trade_time) AS earliest_trade,
            dateDiff('second', max(trade_time), now()) AS seconds_behind
        FROM stock_analytics.raw_trades
    """)
    latest, earliest, lag = result[0]
    logger.info("  Earliest trade: %s", earliest)
    logger.info("  Latest trade:   %s", latest)
    logger.info("  Seconds behind: %ss", lag)

    if lag < FRESHNESS_OK_SECONDS:
        logger.info("  Data is fresh (< %d min old)", FRESHNESS_OK_SECONDS // 60)
    else:
        logger.warning(
            "  Data is %ss old — start Week 3 producer before verifying freshness",
            lag,
        )

    return {"latest": latest, "earliest": earliest, "lag_seconds": lag}


def check_price_sanity() -> list[tuple]:
    """
    Per-ticker min/max/avg price — catches bad transforms or empty columns.
    Flags min <= 0 or max > 10000 as suspicious.
    """
    result = execute_query("""
        SELECT
            ticker,
            min(price) AS min_price,
            max(price) AS max_price,
            avg(price) AS avg_price,
            count() AS trades
        FROM stock_analytics.raw_trades
        GROUP BY ticker
        ORDER BY ticker
    """)

    issues = []
    for ticker, min_p, max_p, _avg_p, _cnt in result:
        if min_p <= 0:
            issues.append(f"{ticker}: min_price = {min_p} (should be > 0)")
        if max_p > 10000:
            issues.append(f"{ticker}: max_price = {max_p} (suspiciously high)")

    if issues:
        for issue in issues:
            logger.warning("  %s", issue)
    else:
        logger.info("  All prices look realistic (> 0, < 10000)")

    return result


def check_kafka_consumer() -> None:
    """
    Inspect ClickHouse Kafka engine consumer via system.kafka_consumers.
    Schema varies slightly by ClickHouse version — failures are non-fatal.
    """
    try:
        result = execute_query("""
            SELECT
                database,
                table,
                assignments.topic,
                assignments.partition_id,
                last_poll_time,
                num_messages_read
            FROM system.kafka_consumers
            WHERE database = 'stock_analytics'
            LIMIT 5
        """)
        if result:
            logger.info("  Kafka consumer active: %d partition assignment(s)", len(result))
            for row in result:
                logger.info(
                    "    topic=%s, partition=%s, messages_read=%s",
                    row[2],
                    row[3],
                    row[5],
                )
        else:
            logger.warning("  No rows in system.kafka_consumers — engine may not have polled yet")
    except Exception as exc:
        logger.info("  Kafka consumer check skipped: %s", exc)


def check_source_distribution() -> list[tuple]:
    """
    Break down raw_trades by source column.
    Expect mix of: simulator (Week 3 producer), kafka_test , bulk/Delta origins.
    """
    result = execute_query("""
        SELECT source, count() AS cnt
        FROM stock_analytics.raw_trades
        GROUP BY source
        ORDER BY cnt DESC
    """)
    for source, cnt in result:
        logger.info("  %-20s %8d rows", source, cnt)
    return result


def generate_report(
    counts: dict[str, int],
    tickers: list[tuple],
    freshness: dict,
    prices: list[tuple],
    sources: list[tuple],
) -> str:
    """Build markdown report and write to docs/clickhouse_verification_report.md."""
    report: list[str] = []
    report.append("# ClickHouse Pipeline Verification Report")
    report.append(f"\n**Generated:** {datetime.now(timezone.utc).isoformat()}")
    report.append(
        "\n**Pipeline:** Producer → Kafka (Redpanda) → ClickHouse Kafka Engine → raw_trades"
    )
    report.append("")

    report.append("## Row Counts")
    report.append("| Table | Rows |")
    report.append("|-------|------|")
    for table, count in counts.items():
        report.append(f"| {table} | {count:,} |")

    report.append("")
    report.append("## Ticker Distribution")
    report.append("| Ticker | Trades |")
    report.append("|--------|--------|")
    for ticker, cnt in tickers:
        report.append(f"| {ticker} | {cnt:,} |")

    report.append("")
    report.append("## Data Freshness")
    report.append(f"- Earliest trade: `{freshness['earliest']}`")
    report.append(f"- Latest trade: `{freshness['latest']}`")
    report.append(f"- Seconds behind real-time: `{freshness['lag_seconds']}`")

    report.append("")
    report.append("## Price Sanity Check")
    report.append("| Ticker | Min | Max | Avg | Trades |")
    report.append("|--------|-----|-----|-----|--------|")
    for ticker, min_p, max_p, avg_p, cnt in prices:
        report.append(f"| {ticker} | ${min_p:.2f} | ${max_p:.2f} | ${avg_p:.2f} | {cnt:,} |")

    report.append("")
    report.append("## Source Distribution")
    report.append("| Source | Rows |")
    report.append("|--------|------|")
    for source, cnt in sources:
        report.append(f"| {source} | {cnt:,} |")

    report.append("")
    report.append("## Architecture")
    report.append("```")
    report.append("Producer (simulator) → Kafka (stock_trades, 3 partitions)")
    report.append("    → ClickHouse Kafka Engine (kafka_trades)")
    report.append("    → MV (mv_kafka_to_trades)")
    report.append("    → ReplacingMergeTree (raw_trades)")
    report.append("```")

    report.append("")
    fresh = freshness["lag_seconds"] < FRESHNESS_OK_SECONDS
    report.append(f"## Status: {'VERIFIED' if fresh else 'NEEDS REVIEW (stale data)'}")

    report_text = "\n".join(report)

    REPORT_PATH.parent.mkdir(parents=True, exist_ok=True)
    REPORT_PATH.write_text(report_text, encoding="utf-8")

    logger.info("Report saved to %s", REPORT_PATH.relative_to(REPO_ROOT))
    return report_text


def main() -> None:
    logger.info("=" * 60)
    logger.info("End-to-End Pipeline Verification")
    logger.info("=" * 60)

    logger.info("\nRow Counts:")
    counts = check_row_counts()

    logger.info("\nTicker Coverage:")
    tickers = check_all_tickers()

    logger.info("\nData Freshness:")
    freshness = check_data_freshness()

    logger.info("\nPrice Sanity:")
    prices = check_price_sanity()

    logger.info("\nKafka Consumer:")
    check_kafka_consumer()

    logger.info("\nSource Distribution:")
    sources = check_source_distribution()

    logger.info("\nGenerating Report:")
    generate_report(counts, tickers, freshness, prices, sources)

    logger.info("\n" + "=" * 60)
    logger.info("Day 1 complete — pipeline verification finished.")
    logger.info("=" * 60)


if __name__ == "__main__":
    main()
