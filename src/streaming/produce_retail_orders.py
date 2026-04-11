from __future__ import annotations

import json
import os
import sys
from pathlib import Path

from kafka import KafkaProducer


PROJECT_ROOT = Path(__file__).resolve().parents[2]
DEFAULT_INPUT_PATH = PROJECT_ROOT / "data" / "processed" / "retail_orders_clean.jsonl"

BROKER = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:19092")
TOPIC = os.getenv("KAFKA_TOPIC", "retail-orders")
INPUT_PATH = Path(os.getenv("RETAIL_ORDERS_INPUT_PATH", str(DEFAULT_INPUT_PATH)))
MAX_MESSAGES = int(os.getenv("MAX_MESSAGES", "25"))


def build_producer() -> KafkaProducer:
    return KafkaProducer(
        bootstrap_servers=[BROKER],
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        key_serializer=lambda v: v.encode("utf-8"),
    )


def load_events(path: Path, max_messages: int) -> list[dict]:
    if not path.exists():
        raise FileNotFoundError(f"Input file not found: {path}")

    events: list[dict] = []
    with path.open("r", encoding="utf-8") as infile:
        for line in infile:
            line = line.strip()
            if not line:
                continue
            events.append(json.loads(line))
            if len(events) >= max_messages:
                break

    if not events:
        raise ValueError(f"No events found in input file: {path}")

    return events


def main() -> int:
    try:
        print(f"Reading events from: {INPUT_PATH}")
        print(f"Sending to broker: {BROKER}")
        print(f"Sending to topic: {TOPIC}")

        events = load_events(INPUT_PATH, MAX_MESSAGES)
        producer = build_producer()

        for event in events:
            order_id = str(event.get("order_id", "unknown-order"))
            producer.send(
                TOPIC,
                key=order_id,
                value=event,
            )
            print(f"Queued event for order_id={order_id}")

        producer.flush()
        producer.close()

        print(f"Successfully sent {len(events)} events to topic '{TOPIC}'")
        return 0

    except Exception as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())