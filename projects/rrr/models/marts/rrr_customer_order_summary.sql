{{ config(tags=['daily', 'customer']) }}

select
    customer_id,
    max(customer_name) as customer_name,
    count(*) as order_count,
    sum(amount) as lifetime_order_amount,
    max(order_date) as most_recent_order_date
from {{ ref('fct_rrr_orders') }}
where order_status <> 'CANCELLED'
group by customer_id

