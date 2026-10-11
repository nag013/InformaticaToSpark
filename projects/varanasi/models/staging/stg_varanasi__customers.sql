with source as (
    select * from {{ source('varanasi_raw', 'customers') }}
),

renamed as (
    select
        cast(customer_id as string) as customer_id,
        trim(customer_name) as customer_name,
        lower(trim(email)) as email,
        cast(updated_at as timestamp) as updated_at
    from source
    where customer_id is not null
)

select * from renamed

