from __future__ import annotations

import json
import os
import sys
from pathlib import Path

import duckdb
import pandas as pd
from kafka import KafkaConsumer
from src.common.log_utils import get_logger, log_event

PROJECT_ROOT = Path(__file__).resolve().parents[2]
DUCKDB_PATH = PROJECT_ROOT / "data" / "warehouse" / "retail.duckdb"

BROKER = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:19092")
TOPIC = os.getenv("KAFKA_TOPIC", "retail-orders")
GROUP_ID = os.getenv("KAFKA_GROUP_ID", "retail-orders-duckdb-loader")
MAX_MESSAGES = int(os.getenv("MAX_MESSAGES", "25"))

logger = get_logger(__name__)


def build_consumer() -> KafkaConsumer:
    return KafkaConsumer(
        TOPIC,
        bootstrap_servers=[BROKER],
        group_id=GROUP_ID,
        auto_offset_reset="earliest",
        enable_auto_commit=True,
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
        key_deserializer=lambda v: v.decode("utf-8") if v else None,
        consumer_timeout_ms=5000,
    )


def ensure_table_exists(conn: duckdb.DuckDBPyConnection) -> None:
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS retail_order_events (
            order_id BIGINT,
            customer_id BIGINT,
            product_id BIGINT,
            category VARCHAR,
            quantity BIGINT,
            unit_price DOUBLE,
            total_amount DOUBLE,
            payment_method VARCHAR,
            order_status VARCHAR,
            region VARCHAR,
            timestamp TIMESTAMP,
            amount_bucket VARCHAR,
            kafka_key VARCHAR
        )
        """
    )


def main() -> int:
    try:
        log_event(
            logger,
            "stream_consumer_started",
            broker=BROKER,
            topic=TOPIC,
            duckdb_path=str(DUCKDB_PATH),
            group_id=GROUP_ID,
        )

        consumer = build_consumer()
        rows: list[dict] = []

        for idx, message in enumerate(consumer):
            event = message.value
            event["kafka_key"] = message.key
            rows.append(event)
            log_event(
                logger,
                "stream_event_consumed",
                order_id=event.get("order_id"),
                topic=TOPIC,
                kafka_key=message.key,
            )

            if idx + 1 >= MAX_MESSAGES:
                break

        consumer.close()

        if not rows:
            raise ValueError("No messages consumed from topic.")

        df = pd.DataFrame(rows)

        with duckdb.connect(str(DUCKDB_PATH)) as conn:
            ensure_table_exists(conn)
            conn.register("events_df", df)
            conn.execute(
                """
                INSERT INTO retail_order_events
                SELECT
                    e.order_id,
                    e.customer_id,
                    e.product_id,
                    e.category,
                    e.quantity,
                    e.unit_price,
                    e.total_amount,
                    e.payment_method,
                    e.order_status,
                    e.region,
                    CAST(e.timestamp AS TIMESTAMP),
                    e.amount_bucket,
                    e.kafka_key
                FROM events_df e
                WHERE NOT EXISTS (
                    SELECT 1
                    FROM retail_order_events r
                    WHERE r.order_id = e.order_id
                )
                """
            )

        log_event(
            logger,
            "stream_consumer_complete",
            target_table="retail_order_events",
            row_count=len(df),
        )
        return 0

    except Exception as exc:
        logger.error("stream_consumer_failed | error=%s", exc)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())