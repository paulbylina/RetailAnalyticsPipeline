SELECT
    order_date,
    COUNT(*) AS total_orders,
    ROUND(SUM(total_amount), 2) AS total_revenue
FROM fact_orders
GROUP BY order_date
ORDER BY order_date DESC;