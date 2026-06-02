"""
Centralized configuration for the ClickHouse analytics service.
All connection details, paths, and tuning parameters in one place.
Other modules import from here — never hardcode values.
"""

from functools import lru_cache
from pathlib import Path

from pydantic_settings import BaseSettings, SettingsConfigDict

_REPO_ROOT = Path(__file__).resolve().parent.parent


class Settings(BaseSettings):
    # ClickHouse
    clickhouse_host: str = "localhost"
    clickhouse_port: int = 8123
    clickhouse_user: str = "default"
    clickhouse_password: str = ""
    clickhouse_database: str = "stock_analytics"

    # ElasticSearch
    es_host: str = "localhost"
    es_port: int = 9200
    es_index_trades: str = "stock-trades"

    # SQL Server (row-store benchmark — Azure SQL Edge on ARM Mac)
    sqlserver_host: str = "localhost"
    sqlserver_port: int = 1433
    sqlserver_user: str = "sa"
    sqlserver_password: str = "StrongP@ss123!"
    sqlserver_database: str = "stock_analytics"

    # Kafka / Redpanda
    kafka_bootstrap_servers: str = "localhost:9092"
    kafka_topic: str = "stock_trades"

    # Data paths (adjust to your machine)
    delta_raw_trades_path: str = "../kafka-spark-streaming-pipeline/data/delta/raw_trades"
    delta_vwap_1min_path: str = "../kafka-spark-streaming-pipeline/data/delta/vwap_1min"
    company_metadata_csv: str = (
        "../data-engineering-portfolio/01-snowflake-incremental-pipeline/data/company_metadata.csv"
    )

    # 25 tickers used across ingestion, API, and dashboard demos.
    tickers: list[str] = [
        "NVDA", "AAPL", "MSFT", "GOOG", "AMZN", "META", "TSLA",
        "JPM", "GS", "MS", "BAC", "BRK-B", "AXP",
        "UNH", "ALL", "PGR", "TRV",
        "SNOW", "CRM", "UBER", "NFLX",
        "V", "MA", "PYPL", "XYZ",
    ]

    model_config = SettingsConfigDict(
        env_file=_REPO_ROOT / ".env",
        env_file_encoding="utf-8",
        extra="ignore",
    )

    @property
    def clickhouse_http_url(self) -> str:
        return f"http://{self.clickhouse_host}:{self.clickhouse_port}"

    @property
    def elasticsearch_url(self) -> str:
        return f"http://{self.es_host}:{self.es_port}"

    def resolve_path(self, path: str) -> Path:
        resolved = Path(path)
        if not resolved.is_absolute():
            resolved = (_REPO_ROOT / resolved).resolve()
        return resolved


@lru_cache
def get_settings() -> Settings:
    return Settings()
