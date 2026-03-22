import json
import random
from datetime import datetime, UTC, timedelta
from pathlib import Path

OUTPUT_FILE = Path(__file__).resolve().parents[2] / "data" / "raw" / "retail_orders.jsonl"

PRODUCT_CATEGORIES = ["electronics", "clothing", "home", "beauty", "sports", "grocery"]
PAYMENT_METHODS = ["credit_card", "debit_card", "paypal", "apple_pay"]
ORDER_STATUSES = ["completed", "pending", "cancelled", "returned"]
REGIONS = ["south", "midwest", "northeast", "west"]

def generate_order(order_id: int):
    quantity = random.randint(1, 5)
    unit_price = round(random.uniform(8.0, 250.0), 2)
    event_time = datetime.now(UTC) - timedelta(minutes=random.randint(0, 10080))

    return {
        "order_id": order_id,
        "customer_id": random.randint(1, 500),
        "product_id": random.randint(10000, 99999),
        "category": random.choice(PRODUCT_CATEGORIES),
        "quantity": quantity,
        "unit_price": unit_price,
        "total_amount": round(quantity * unit_price, 2),
        "payment_method": random.choice(PAYMENT_METHODS),
        "order_status": random.choice(ORDER_STATUSES),
        "region": random.choice(REGIONS),
        "timestamp": event_time.isoformat()
    }

def main():
    OUTPUT_FILE.parent.mkdir(parents=True, exist_ok=True)

    with open(OUTPUT_FILE, "w") as f:
        for order_id in range(1, 501):
            order = generate_order(order_id)
            f.write(json.dumps(order) + "\n")

    print(f"Generated 500 retail orders in {OUTPUT_FILE}")

if __name__ == "__main__":
    main()