import json
from collections import defaultdict
from pathlib import Path

PROCESSED_FILE = Path(__file__).resolve().parents[2] / "data" / "processed" / "retail_orders_clean.jsonl"
AGGREGATE_FILE = Path(__file__).resolve().parents[2] / "data" / "processed" / "retail_order_summary.json"

def main():
    total_orders = 0
    total_revenue = 0.0
    revenue_by_category = defaultdict(float)
    orders_by_status = defaultdict(int)
    revenue_by_region = defaultdict(float)

    with open(PROCESSED_FILE, "r") as f:
        for line in f:
            order = json.loads(line)
            total_orders += 1
            total_revenue += order["total_amount"]
            revenue_by_category[order["category"]] += order["total_amount"]
            orders_by_status[order["order_status"]] += 1
            revenue_by_region[order["region"]] += order["total_amount"]

    summary = {
        "total_orders": total_orders,
        "total_revenue": round(total_revenue, 2),
        "revenue_by_category": {k: round(v, 2) for k, v in revenue_by_category.items()},
        "orders_by_status": dict(orders_by_status),
        "revenue_by_region": {k: round(v, 2) for k, v in revenue_by_region.items()}
    }

    with open(AGGREGATE_FILE, "w") as f:
        json.dump(summary, f, indent=2)

    print(f"Retail summary written to {AGGREGATE_FILE}")

if __name__ == "__main__":
    main()