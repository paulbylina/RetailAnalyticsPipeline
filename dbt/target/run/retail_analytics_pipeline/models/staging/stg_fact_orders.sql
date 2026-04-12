
  
  create view "retail"."main"."stg_fact_orders__dbt_tmp" as (
    select
    cast(order_id as bigint) as order_id,
    cast(customer_id as bigint) as customer_id,
    cast(product_id as bigint) as product_id,
    cast(category as varchar) as category,
    cast(quantity as bigint) as quantity,
    cast(unit_price as double) as unit_price,
    cast(total_amount as double) as total_amount,
    cast(payment_method as varchar) as payment_method,
    cast(order_status as varchar) as order_status,
    cast(region as varchar) as region,
    cast(order_date as date) as order_date,
    cast(amount_bucket as varchar) as amount_bucket
from "retail"."main"."fact_orders"
  );
