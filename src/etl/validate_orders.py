import json
from pathlib import Path

PROCESSED_FILE = Path(__file__).resolve().parents[2] / "data" / "processed" / "orders_clean.jsonl"

def is_valid_order(order):
    required_fields = ["order_id", "customer_id", "amount", "timestamp", "amount_bucket"]

    for field in required_fields:
        if field not in order:
            return False, f"Missing field: {field}"

    if order["amount"] < 0:
        return False, "Amount cannot be negative"

    if order["amount_bucket"] not in {"low", "medium", "high"}:
        return False, "Invalid amount_bucket"

    return True, "OK"

def main():
    total = 0
    valid = 0
    invalid = 0

    with open(PROCESSED_FILE, "r") as f:
        for line in f:
            total += 1
            order = json.loads(line)
            ok, reason = is_valid_order(order)
            if ok:
                valid += 1
            else:
                invalid += 1
                print(f"Invalid record: {reason} | {order}")

    print(f"Total records: {total}")
    print(f"Valid records: {valid}")
    print(f"Invalid records: {invalid}")

if __name__ == "__main__":
    main()