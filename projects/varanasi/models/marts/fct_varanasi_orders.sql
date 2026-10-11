{{ config(tags=['daily', 'orders']) }}

select
    {{ stable_key(['order_id']) }} as order_key,
    order_id,
    customer_id,
    customer_name,
    order_date,
    amount,
    order_status,
    updated_at,
    current_timestamp as dbt_loaded_at
from {{ ref('int_varanasi__order_customer') }}

