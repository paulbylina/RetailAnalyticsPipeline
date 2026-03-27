SELECT
    dd.weekday_name,
    COUNT(*) AS total_orders,
    ROUND(SUM(fo.total_amount), 2) AS total_revenue,
    ROUND(AVG(fo.total_amount), 2) AS avg_order_value
FROM fact_orders fo
JOIN dim_date dd
    ON fo.order_date = dd.full_date
GROUP BY dd.weekday_name
ORDER BY total_revenue DESC;