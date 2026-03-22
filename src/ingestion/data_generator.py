import json
import random
import time
from datetime import datetime
from pathlib import Path

OUTPUT_FILE = Path(__file__).resolve().parents[2] / "data" / "raw" / "orders.jsonl"

def generate_order():
    return {
        "order_id": random.randint(1000, 9999),
        "customer_id": random.randint(1, 100),
        "amount": round(random.uniform(10.0, 500.0), 2),
        "timestamp": datetime.utcnow().isoformat()
    }

def main():
    OUTPUT_FILE.parent.mkdir(parents=True, exist_ok=True)
    print(f"Generating data into {OUTPUT_FILE} ...")

    with open(OUTPUT_FILE, "a") as f:
        for _ in range(100):
            order = generate_order()
            f.write(json.dumps(order) + "\n")
            time.sleep(0.1)

    print("Done generating data.")

if __name__ == "__main__":
    main()