"""
Dagster assets for the Stock Analytics Data Service.

Dependency chain (downstream blocked on upstream failure):
  check_infrastructure → verify_ingestion → run_quality_checks
      → refresh_materialized_views → refresh_sector_summary → generate_benchmark_report

Reuses existing src/ modules — Dagster enforces order and surfaces metadata in the UI.
"""

import logging
import subprocess
import sys
import time
from pathlib import Path

from dagster import AssetExecutionContext, MaterializeResult, MetadataValue, asset

from src.config import get_settings

logger = logging.getLogger(__name__)

_REPO_ROOT = Path(__file__).resolve().parent.parent

_MV_TABLES = ("mv_realtime_vwap", "mv_daily_summary", "mv_hourly_stats")


@asset(
    group_name="data_service",
    description="Verify ClickHouse, ElasticSearch, and Redpanda are reachable.",
)
def check_infrastructure(context: AssetExecutionContext) -> MaterializeResult:
    """Ping critical services before any data work runs."""
    settings = get_settings()
    results: dict[str, str] = {}

    try:
        from src.clickhouse_client import execute_query

        version = execute_query("SELECT version()")[0][0]
        results["clickhouse"] = f"healthy (v{version})"
    except Exception as exc:
        raise RuntimeError(f"ClickHouse unhealthy: {exc}") from exc

    try:
        from src.es_client import get_es_client

        health = get_es_client().cluster.health()
        results["elasticsearch"] = f"healthy ({health['status']})"
    except Exception as exc:
        raise RuntimeError(f"ElasticSearch unhealthy: {exc}") from exc

    try:
        from confluent_kafka.admin import AdminClient

        admin = AdminClient({"bootstrap.servers": settings.kafka_bootstrap_servers})
        metadata = admin.list_topics(timeout=10)
        broker_count = len(metadata.brokers)
        results["redpanda"] = f"healthy ({broker_count} broker(s))"
    except Exception as exc:
        # Optional for local dev when Kafka is not running — log but do not block
        results["redpanda"] = f"skipped ({exc})"
        context.log.warning("Redpanda check skipped: %s", exc)

    context.log.info("Infrastructure: %s", results)
    return MaterializeResult(
        metadata={key: MetadataValue.text(value) for key, value in results.items()}
    )


@asset(
    group_name="data_service",
    deps=[check_infrastructure],
    description="Confirm ClickHouse and ElasticSearch contain trade data.",
)
def verify_ingestion(context: AssetExecutionContext) -> MaterializeResult:
    """Row counts and latest timestamp — empty CH means bulk load was never run."""
    from src.clickhouse_client import execute_query
    from src.es_client import get_es_client

    ch_count = int(execute_query("SELECT count() FROM stock_analytics.raw_trades")[0][0])
    ch_latest = execute_query("SELECT max(trade_time) FROM stock_analytics.raw_trades")[0][0]

    if ch_count == 0:
        raise RuntimeError("ClickHouse raw_trades is empty — run bulk loader first")

    index_name = get_settings().es_index_trades
    es = get_es_client()
    es.indices.refresh(index=index_name)
    es_count = int(es.count(index=index_name)["count"])

    context.log.info("CH rows=%s, ES docs=%s, latest=%s", ch_count, es_count, ch_latest)
    return MaterializeResult(
        metadata={
            "ch_row_count": MetadataValue.int(ch_count),
            "es_doc_count": MetadataValue.int(es_count),
            "ch_latest_trade": MetadataValue.text(str(ch_latest)),
            "counts_match": MetadataValue.bool(ch_count == es_count),
        }
    )


@asset(
    group_name="data_service",
    deps=[verify_ingestion],
    description="Seven cross-engine quality checks; blocks MV refresh and benchmarks on failure.",
)
def run_quality_checks(context: AssetExecutionContext) -> MaterializeResult:
    """Gate asset — delegates to src.data_quality_checks (same logic as CLI)."""
    from src.data_quality_checks import DataQualityFailure, execute_all_checks

    try:
        results = execute_all_checks()
    except DataQualityFailure as exc:
        context.log.error("Quality checks failed: %s", exc)
        raise RuntimeError(str(exc)) from exc

    passed = sum(1 for _, ok, _ in results if ok)
    metadata: dict[str, MetadataValue] = {
        "checks_passed": MetadataValue.int(passed),
        "checks_failed": MetadataValue.int(len(results) - passed),
        "report_path": MetadataValue.path("docs/data_quality_report.md"),
    }
    for name, _, msg in results:
        key = name.lower().replace(" ", "_").replace("(", "").replace(")", "")[:40]
        metadata[key] = MetadataValue.text(msg)

    context.log.info("All %d quality checks passed", passed)
    return MaterializeResult(metadata=metadata)


@asset(
    group_name="data_service",
    deps=[run_quality_checks],
    description="OPTIMIZE materialized views to merge ClickHouse data parts.",
)
def refresh_materialized_views(context: AssetExecutionContext) -> MaterializeResult:
    """Trigger FINAL merge on AggregatingMergeTree / SummingMergeTree MV targets."""
    from src.clickhouse_client import execute_command, execute_query

    row_counts: dict[str, int | str] = {}

    for table in _MV_TABLES:
        try:
            execute_command(f"OPTIMIZE TABLE stock_analytics.{table} FINAL")
            row_counts[table] = int(
                execute_query(f"SELECT count() FROM stock_analytics.{table}")[0][0]
            )
            context.log.info("OPTIMIZE %s → %s rows", table, row_counts[table])
        except Exception as exc:
            row_counts[table] = f"error: {exc}"
            context.log.warning("OPTIMIZE %s failed: %s", table, exc)

    metadata: dict[str, MetadataValue] = {}
    for table, value in row_counts.items():
        if isinstance(value, int):
            metadata[table] = MetadataValue.int(value)
        else:
            metadata[table] = MetadataValue.text(str(value))

    return MaterializeResult(metadata=metadata)


@asset(
    group_name="data_service",
    deps=[refresh_materialized_views],
    description="Rebuild sector_summary (JOIN-based table, not an MV).",
)
def refresh_sector_summary(context: AssetExecutionContext) -> MaterializeResult:
    """Truncate and repopulate sector rollups from raw_trades + company_metadata."""
    from src.clickhouse_client import execute_command, execute_query

    execute_command("TRUNCATE TABLE stock_analytics.sector_summary")
    execute_command(
        """
        INSERT INTO stock_analytics.sector_summary
        SELECT
            m.sector,
            toDate(t.trade_time) AS trade_date,
            uniqExact(t.ticker) AS ticker_count,
            count() AS trade_count,
            sum(t.quantity) AS total_volume,
            avg(t.price) AS avg_price,
            sum(t.price * t.quantity) AS total_notional
        FROM stock_analytics.raw_trades t
        INNER JOIN stock_analytics.company_metadata m ON t.ticker = m.ticker
        GROUP BY m.sector, toDate(t.trade_time)
        """
    )

    count = int(execute_query("SELECT count() FROM stock_analytics.sector_summary")[0][0])
    context.log.info("sector_summary refreshed: %s rows", count)
    return MaterializeResult(metadata={"sector_summary_rows": MetadataValue.int(count)})


@asset(
    group_name="data_service",
    deps=[refresh_sector_summary],
    description="Run benchmark suite and write docs/query_optimization_report.md.",
)
def generate_benchmark_report(context: AssetExecutionContext) -> MaterializeResult:
    """Subprocess wrapper around src.benchmark_suite — same as manual CLI run."""
    start = time.perf_counter()
    result = subprocess.run(
        [sys.executable, "-m", "src.benchmark_suite"],
        cwd=_REPO_ROOT,
        capture_output=True,
        text=True,
        timeout=300,
        check=False,
    )
    elapsed = time.perf_counter() - start

    if result.stdout:
        context.log.info(result.stdout[-2000:])
    if result.returncode != 0:
        stderr_snippet = (result.stderr or "")[:500]
        context.log.warning(
            "Benchmark suite exit code %s: %s", result.returncode, stderr_snippet
        )
        raise RuntimeError(
            f"benchmark_suite failed (code {result.returncode}): {stderr_snippet}"
        )

    return MaterializeResult(
        metadata={
            "elapsed_seconds": MetadataValue.float(round(elapsed, 1)),
            "return_code": MetadataValue.int(result.returncode),
            "report_path": MetadataValue.path("docs/query_optimization_report.md"),
        }
    )
