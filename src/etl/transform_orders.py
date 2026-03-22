import json
from pathlib import Path

RAW_FILE = Path(__file__).resolve().parents[2] / "data" / "raw" / "orders.jsonl"
PROCESSED_FILE = Path(__file__).resolve().parents[2] / "data" / "processed" / "orders_clean.jsonl"

def clean_order(order):
    return {
        "order_id": int(order["order_id"]),
        "customer_id": int(order["customer_id"]),
        "amount": float(order["amount"]),
        "timestamp": order["timestamp"],
        "amount_bucket": (
            "low" if float(order["amount"]) < 100
            else "medium" if float(order["amount"]) < 300
            else "high"
        )
    }

def main():
    PROCESSED_FILE.parent.mkdir(parents=True, exist_ok=True)

    with open(RAW_FILE, "r") as infile, open(PROCESSED_FILE, "w") as outfile:
        for line in infile:
            order = json.loads(line)
            cleaned = clean_order(order)
            outfile.write(json.dumps(cleaned) + "\n")

    print(f"Cleaned data written to {PROCESSED_FILE}")

if __name__ == "__main__":
    main()