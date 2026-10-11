with orders as (
    select * from {{ ref('stg_rrr__orders') }}
),

customers as (
    select * from {{ ref('stg_rrr__customers') }}
)

select
    o.order_id,
    o.customer_id,
    c.customer_name,
    c.email,
    o.order_date,
    o.amount,
    o.order_status,
    o.updated_at
from orders o
left join customers c on o.customer_id = c.customer_id

