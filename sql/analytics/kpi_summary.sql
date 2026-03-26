SELECT
    COUNT(*) AS total_orders,
    ROUND(SUM(total_amount), 2) AS total_revenue,
    ROUND(AVG(total_amount), 2) AS avg_order_value,
    ROUND(SUM(CASE WHEN order_status = 'completed' THEN total_amount ELSE 0 END), 2) AS completed_revenue,
    SUM(CASE WHEN order_status = 'cancelled' THEN 1 ELSE 0 END) AS cancelled_orders,
    SUM(CASE WHEN order_status = 'returned' THEN 1 ELSE 0 END) AS returned_orders
FROM fact_orders;