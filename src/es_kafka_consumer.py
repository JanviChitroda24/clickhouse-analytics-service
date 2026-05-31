"""
Kafka consumer that indexes trades into ElasticSearch in real-time (Hour 12).

Runs alongside ClickHouse's native Kafka engine — both read stock_trades topic.
Different consumer groups mean each engine gets every message independently:

  clickhouse_consumer  -> ClickHouse Kafka engine (Hour 4)
  es_consumer          -> this script -> ES bulk index

Batching: flush at 100 documents OR 5 seconds (whichever comes first).

Usage:
    python -m src.es_kafka_consumer
    (runs until Ctrl+C)
"""

import json
import logging
import signal
import time

from confluent_kafka import Consumer, KafkaError
from elasticsearch import helpers

from src.config import get_settings
from src.es_client import get_es_client
from src.es_document import build_es_doc, load_metadata

logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")
logger = logging.getLogger(__name__)

ES_CONSUMER_GROUP = "es_consumer"
BATCH_MAX_DOCS = 100
BATCH_MAX_SECONDS = 5

running = True


def _signal_handler(sig, frame) -> None:
    global running
    logger.info("Shutting down...")
    running = False


signal.signal(signal.SIGINT, _signal_handler)
signal.signal(signal.SIGTERM, _signal_handler)


def flush_batch(es_client, batch: list) -> int:
    """Bulk-index batch; return number of successful documents."""
    if not batch:
        return 0
    success, errors = helpers.bulk(es_client, batch, raise_on_error=False)
    if errors:
        logger.warning("%s bulk errors (showing first 3)", len(errors))
        for err in errors[:3]:
            logger.warning("  %s", err)
    return success


def main() -> None:
    settings = get_settings()
    es_client = get_es_client()
    metadata = load_metadata()
    logger.info("Loaded metadata for %s tickers", len(metadata))

    consumer = Consumer(
        {
            "bootstrap.servers": settings.kafka_bootstrap_servers,
            "group.id": ES_CONSUMER_GROUP,
            "auto.offset.reset": "latest",
            "enable.auto.commit": True,
        }
    )
    consumer.subscribe([settings.kafka_topic])

    logger.info("Consuming from '%s' (group: %s)", settings.kafka_topic, ES_CONSUMER_GROUP)
    logger.info("Batch: %s docs or %ss — Ctrl+C to stop", BATCH_MAX_DOCS, BATCH_MAX_SECONDS)

    batch: list = []
    last_flush = time.time()
    total_indexed = 0

    while running:
        msg = consumer.poll(1.0)

        if msg is None:
            if batch and (time.time() - last_flush) >= BATCH_MAX_SECONDS:
                success = flush_batch(es_client, batch)
                total_indexed += success
                logger.info("Flushed %s docs (timer) | total: %s", success, total_indexed)
                batch = []
                last_flush = time.time()
            continue

        if msg.error():
            if msg.error().code() == KafkaError._PARTITION_EOF:
                continue
            logger.error("Kafka error: %s", msg.error())
            continue

        try:
            trade = json.loads(msg.value().decode("utf-8"))
            batch.append(build_es_doc(trade, metadata))
        except Exception as exc:
            logger.error("Parse error: %s", exc)
            continue

        if len(batch) >= BATCH_MAX_DOCS:
            success = flush_batch(es_client, batch)
            total_indexed += success
            logger.info("Flushed %s docs (batch) | total: %s", success, total_indexed)
            batch = []
            last_flush = time.time()

    if batch:
        success = flush_batch(es_client, batch)
        total_indexed += success
        logger.info("Final flush: %s docs | total: %s", success, total_indexed)

    consumer.close()
    logger.info("Consumer stopped. Total indexed: %s", total_indexed)


if __name__ == "__main__":
    main()
