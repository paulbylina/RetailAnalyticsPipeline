import json
from pathlib import Path

PROCESSED_FILE = Path(__file__).resolve().parents[2] / "data" / "processed" / "retail_orders_clean.jsonl"

VALID_CATEGORIES = {"electronics", "clothing", "home", "beauty", "sports", "grocery"}
VALID_PAYMENT_METHODS = {"credit_card", "debit_card", "paypal", "apple_pay"}
VALID_ORDER_STATUSES = {"completed", "pending", "cancelled", "returned"}
VALID_REGIONS = {"south", "midwest", "northeast", "west"}
VALID_AMOUNT_BUCKETS = {"low", "medium", "high"}

def is_valid_order(order):
    required_fields = {
        "order_id", "customer_id", "product_id", "category", "quantity",
        "unit_price", "total_amount", "payment_method", "order_status",
        "region", "timestamp", "amount_bucket"
    }

    missing = required_fields - order.keys()
    if missing:
        return False, f"Missing fields: {sorted(missing)}"

    if order["category"] not in VALID_CATEGORIES:
        return False, "Invalid category"

    if order["payment_method"] not in VALID_PAYMENT_METHODS:
        return False, "Invalid payment_method"

    if order["order_status"] not in VALID_ORDER_STATUSES:
        return False, "Invalid order_status"

    if order["region"] not in VALID_REGIONS:
        return False, "Invalid region"

    if order["amount_bucket"] not in VALID_AMOUNT_BUCKETS:
        return False, "Invalid amount_bucket"

    if order["quantity"] <= 0:
        return False, "Quantity must be positive"

    if order["unit_price"] <= 0:
        return False, "Unit price must be positive"

    if order["total_amount"] <= 0:
        return False, "Total amount must be positive"

    expected_total = round(order["quantity"] * order["unit_price"], 2)
    if round(order["total_amount"], 2) != expected_total:
        return False, "Total amount does not match quantity * unit_price"

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