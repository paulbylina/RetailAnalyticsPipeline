SELECT
    category,
    COUNT(*) AS total_orders,
    ROUND(SUM(total_amount), 2) AS total_revenue,
    ROUND(AVG(total_amount), 2) AS avg_order_value
FROM fact_orders
GROUP BY category
ORDER BY total_revenue DESC;