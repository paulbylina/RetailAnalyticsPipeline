SELECT
    order_status,
    COUNT(*) AS total_orders,
    ROUND(SUM(total_amount), 2) AS total_revenue,
    ROUND(AVG(total_amount), 2) AS avg_order_value
FROM fact_orders
GROUP BY order_status
ORDER BY total_orders DESC;