from src.etl.transform_retail_orders import clean_order


def test_clean_order_normalizes_fields_and_adds_amount_bucket():
    raw_order = {
        "order_id": "1",
        "customer_id": "101",
        "product_id": "50001",
        "category": " Electronics ",
        "quantity": 2,
        "unit_price": 25.0,
        "total_amount": 50.0,
        "payment_method": " Credit_Card ",
        "order_status": " Completed ",
        "region": " South ",
        "timestamp": "2026-03-20T12:00:00+00:00",
    }

    cleaned = clean_order(raw_order)

    assert cleaned["order_id"] == 1
    assert cleaned["customer_id"] == 101
    assert cleaned["product_id"] == 50001
    assert cleaned["category"] == "electronics"
    assert cleaned["quantity"] == 2
    assert cleaned["unit_price"] == 25.0
    assert cleaned["total_amount"] == 50.0
    assert cleaned["payment_method"] == "credit_card"
    assert cleaned["order_status"] == "completed"
    assert cleaned["region"] == "south"
    assert cleaned["timestamp"] == "2026-03-20T12:00:00+00:00"
    assert cleaned["amount_bucket"] == "medium"