SELECT
    dc.customer_segment,
    COUNT(*) AS total_orders,
    ROUND(SUM(fo.total_amount), 2) AS total_revenue,
    ROUND(AVG(fo.total_amount), 2) AS avg_order_value
FROM fact_orders fo
JOIN dim_customers dc
    ON fo.customer_id = dc.customer_id
GROUP BY dc.customer_segment
ORDER BY total_revenue DESC;