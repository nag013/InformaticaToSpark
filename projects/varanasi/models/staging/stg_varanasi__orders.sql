{{ config(
    materialized='incremental',
    unique_key='order_id',
    incremental_strategy='merge',
    on_schema_change='fail'
) }}

with source as (
    select * from {{ source('varanasi_raw', 'orders') }}
    {% if is_incremental() %}
    where updated_at > (
        select coalesce(max(updated_at), cast('{{ var("load_start_date") }}' as timestamp))
        from {{ this }}
    )
    {% endif %}
),

standardized as (
    select
        cast(order_id as string) as order_id,
        cast(customer_id as string) as customer_id,
        cast(order_date as date) as order_date,
        cast(amount as decimal(18, 2)) as amount,
        upper(trim(status)) as order_status,
        cast(updated_at as timestamp) as updated_at
    from source
    where order_id is not null
)

select * from standardized

