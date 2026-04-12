
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select total_orders
from "retail"."main"."customer_order_summary"
where total_orders is null



  
  
      
    ) dbt_internal_test