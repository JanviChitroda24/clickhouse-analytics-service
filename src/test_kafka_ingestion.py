"""
Test Kafka → ClickHouse real-time ingestion.

Sends test events to the same stock_trades topic as Week 3, then verifies
raw_trades row count increases (ClickHouse Kafka engine + MV must be running).

JSON schema matches kafka-spark-streaming-pipeline/src/trade_simulator.py:
  trade_id, ticker, price, quantity, timestamp, side, trade_type,
  bid_price, ask_price, exchange, source

Usage:
    python -m src.test_kafka_ingestion
"""

import json
import logging
import random
import time
import uuid
from datetime import datetime, timezone

from confluent_kafka import Producer

from src.clickhouse_client import execute_query
from src.config import get_settings

logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")
logger = logging.getLogger(__name__)

# Match trade_simulator.py base prices (subset — unknown tickers default to 100)
BASE_PRICES = {
    "NVDA": 140,
    "AAPL": 210,
    "MSFT": 470,
    "GOOG": 185,
    "AMZN": 215,
    "META": 530,
    "TSLA": 280,
    "JPM": 230,
    "GS": 480,
    "MS": 120,
    "BAC": 40,
    "BRK-B": 475,
    "AXP": 250,
    "UNH": 520,
    "ALL": 170,
    "PGR": 220,
    "TRV": 225,
    "SNOW": 175,
    "CRM": 270,
    "UBER": 75,
    "NFLX": 620,
    "V": 310,
    "MA": 530,
    "PYPL": 70,
    "XYZ": 75,
}


def generate_trade(ticker: str) -> dict:
    """Single trade event — field names must match kafka_trades table / producer JSON."""
    base = BASE_PRICES.get(ticker, 100)
    price = round(base * (1 + random.gauss(0, 0.001)), 2)
    spread = round(price * random.uniform(0.0001, 0.0005), 2)

    return {
        "trade_id": str(uuid.uuid4()),
        "ticker": ticker,
        "price": price,
        "quantity": random.choice([10, 50, 100, 200, 500]),
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "side": random.choice(["BUY", "SELL"]),
        "trade_type": random.choice(["odd_lot", "round_lot", "block"]),
        "bid_price": round(price - spread, 2),
        "ask_price": round(price + spread, 2),
        "exchange": random.choice(["NYSE", "NASDAQ", "ARCA"]),
        "source": "kafka_test",
    }


def delivery_report(err, msg):
    if err:
        logger.error("Delivery failed: %s", err)


def main():
    settings = get_settings()
    num_events = 500
    tickers = settings.tickers

    before = execute_query("SELECT count() FROM stock_analytics.raw_trades")[0][0]
    logger.info("raw_trades count BEFORE: %s", before)

    producer = Producer({"bootstrap.servers": settings.kafka_bootstrap_servers})

    logger.info("Sending %s events to topic '%s'...", num_events, settings.kafka_topic)
    for i in range(num_events):
        ticker = random.choice(tickers)
        trade = generate_trade(ticker)
        producer.produce(
            settings.kafka_topic,
            key=ticker,
            value=json.dumps(trade).encode("utf-8"),
            callback=delivery_report,
        )
        if i % 100 == 0:
            producer.flush()

    producer.flush()
    logger.info("Sent %s events", num_events)

    logger.info("Waiting 10 seconds for ClickHouse Kafka consumer...")
    time.sleep(10)

    after = execute_query("SELECT count() FROM stock_analytics.raw_trades")[0][0]
    new_rows = after - before
    logger.info("raw_trades count AFTER: %s", after)
    logger.info("New rows: %s", new_rows)

    if new_rows >= num_events * 0.9:
        logger.info("Kafka → ClickHouse OK: %s/%s events ingested", new_rows, num_events)
    elif new_rows > 0:
        logger.warning("Partial ingestion: %s/%s — consumer may still be catching up", new_rows, num_events)
    else:
        logger.error("No new rows. Check Kafka engine, MV, and redpanda:29092 networking.")
        logger.error(
            'Debug: docker exec clickhouse clickhouse-client --query '
            '"SELECT * FROM system.kafka_consumers FORMAT Pretty"'
        )

    logger.info("")
    logger.info("Recent trades (source = 'kafka_test'):")
    sample = execute_query("""
        SELECT ticker, price, quantity, side, trade_time, source
        FROM stock_analytics.raw_trades
        WHERE source = 'kafka_test'
        ORDER BY trade_time DESC
        LIMIT 5
    """)
    for row in sample:
        logger.info("  %s", row)


if __name__ == "__main__":
    main()
