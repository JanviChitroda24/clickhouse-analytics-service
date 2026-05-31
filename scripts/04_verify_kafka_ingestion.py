#!/usr/bin/env python3
"""
Verify Kafka engine objects exist and consumer is active.

Run after: python -m src.setup_clickhouse (with Kafka SQL)
         python -m src.test_kafka_ingestion (optional)

Usage:
    python3 scripts/04_verify_kafka_ingestion.py
"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

EXPECTED = {"kafka_trades", "mv_kafka_to_trades"}


def main() -> int:
    try:
        from src.clickhouse_client import execute_query

        rows = execute_query("""
            SELECT name, engine
            FROM system.tables
            WHERE database = 'stock_analytics'
            ORDER BY name
        """)

        print("--- stock_analytics objects ---")
        found = set()
        for name, engine in rows:
            print(f"  {name:<30} {engine}")
            found.add(name)

        missing = EXPECTED - found
        if missing:
            print(f"\nMISSING: {missing}")
            print("Run: python -m src.setup_clickhouse")
            return 1

        print("\nKafka engine + MV: OK")

        try:
            consumers = execute_query("""
                SELECT table, consumer_id, assignments.topic
                FROM system.kafka_consumers
                WHERE database = 'stock_analytics'
                LIMIT 5
            """)
            if consumers:
                print("\nKafka consumers (sample):")
                for row in consumers:
                    print(f"  {row}")
            else:
                print("\nNote: no consumer rows yet (normal until first poll)")
        except Exception as exc:
            print(f"\nCould not query system.kafka_consumers: {exc}")

        return 0
    except Exception as exc:
        print(f"FAILED: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    sys.exit(main())
