SELECT
    dd.weekday_name,
    CASE dd.weekday_name
        WHEN 'Monday' THEN 1
        WHEN 'Tuesday' THEN 2
        WHEN 'Wednesday' THEN 3
        WHEN 'Thursday' THEN 4
        WHEN 'Friday' THEN 5
        WHEN 'Saturday' THEN 6
        WHEN 'Sunday' THEN 7
    END AS weekday_order,
    COUNT(*) AS total_orders,
    ROUND(SUM(fo.total_amount), 2) AS total_revenue,
    ROUND(AVG(fo.total_amount), 2) AS avg_order_value
FROM fact_orders fo
JOIN dim_date dd
    ON fo.order_date = dd.full_date
GROUP BY dd.weekday_name
ORDER BY weekday_order;