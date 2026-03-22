from src.etl.transform_retail_orders import clean_order

def test_clean_order_normalizes_and_enriches_fields():
    raw_order = {
        "order_id": "101",
        "customer_id": "22",
        "product_id": "55555",
        "category": " Electronics ",
        "quantity": "2",
        "unit_price": "49.99",
        "total_amount": "99.98",
        "payment_method": " PayPal ",
        "order_status": " Completed ",
        "region": " South ",
        "timestamp": "2026-03-22T12:00:00+00:00"
    }

    cleaned = clean_order(raw_order)

    assert cleaned["order_id"] == 101
    assert cleaned["customer_id"] == 22
    assert cleaned["product_id"] == 55555
    assert cleaned["category"] == "electronics"
    assert cleaned["quantity"] == 2
    assert cleaned["unit_price"] == 49.99
    assert cleaned["total_amount"] == 99.98
    assert cleaned["payment_method"] == "paypal"
    assert cleaned["order_status"] == "completed"
    assert cleaned["region"] == "south"
    assert cleaned["amount_bucket"] == "medium"