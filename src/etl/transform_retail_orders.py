import json
from pathlib import Path

RAW_FILE = Path(__file__).resolve().parents[2] / "data" / "raw" / "retail_orders.jsonl"
PROCESSED_FILE = Path(__file__).resolve().parents[2] / "data" / "processed" / "retail_orders_clean.jsonl"

def get_amount_bucket(total_amount: float) -> str:
    if total_amount < 50:
        return "low"
    if total_amount < 200:
        return "medium"
    return "high"

def clean_order(order):
    quantity = int(order["quantity"])
    unit_price = float(order["unit_price"])
    total_amount = float(order["total_amount"])

    return {
        "order_id": int(order["order_id"]),
        "customer_id": int(order["customer_id"]),
        "product_id": int(order["product_id"]),
        "category": order["category"].strip().lower(),
        "quantity": quantity,
        "unit_price": unit_price,
        "total_amount": total_amount,
        "payment_method": order["payment_method"].strip().lower(),
        "order_status": order["order_status"].strip().lower(),
        "region": order["region"].strip().lower(),
        "timestamp": order["timestamp"],
        "amount_bucket": get_amount_bucket(total_amount)
    }

def main():
    PROCESSED_FILE.parent.mkdir(parents=True, exist_ok=True)

    with open(RAW_FILE, "r") as infile, open(PROCESSED_FILE, "w") as outfile:
        for line in infile:
            order = json.loads(line)
            cleaned = clean_order(order)
            outfile.write(json.dumps(cleaned) + "\n")

    print(f"Cleaned retail data written to {PROCESSED_FILE}")

if __name__ == "__main__":
    main()