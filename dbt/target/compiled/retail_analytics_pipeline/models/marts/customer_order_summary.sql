select
    customer_id,
    count(*) as total_orders,
    round(sum(total_amount), 2) as total_revenue,
    min(order_date) as first_order_date,
    max(order_date) as last_order_date
from "retail"."main"."stg_fact_orders"
group by customer_id