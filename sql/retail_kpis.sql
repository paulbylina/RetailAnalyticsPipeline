SELECT
    category,
    region,
    COUNT(*) AS total_orders,
    ROUND(SUM(total_amount), 2) AS total_revenue,
    ROUND(AVG(total_amount), 2) AS avg_order_value
FROM retail_orders_clean
WHERE order_status = 'completed'
GROUP BY category, region
ORDER BY total_revenue DESC;