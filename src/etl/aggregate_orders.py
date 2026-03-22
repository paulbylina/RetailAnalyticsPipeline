import json
from collections import defaultdict
from pathlib import Path

PROCESSED_FILE = Path(__file__).resolve().parents[2] / "data" / "processed" / "orders_clean.jsonl"
AGGREGATE_FILE = Path(__file__).resolve().parents[2] / "data" / "processed" / "order_summary.json"

def main():
    total_orders = 0
    total_revenue = 0.0
    orders_by_bucket = defaultdict(int)

    with open(PROCESSED_FILE, "r") as f:
        for line in f:
            order = json.loads(line)
            total_orders += 1
            total_revenue += order["amount"]
            orders_by_bucket[order["amount_bucket"]] += 1

    summary = {
        "total_orders": total_orders,
        "total_revenue": round(total_revenue, 2),
        "orders_by_bucket": dict(orders_by_bucket)
    }

    with open(AGGREGATE_FILE, "w") as f:
        json.dump(summary, f, indent=2)

    print(f"Summary written to {AGGREGATE_FILE}")

if __name__ == "__main__":
    main()