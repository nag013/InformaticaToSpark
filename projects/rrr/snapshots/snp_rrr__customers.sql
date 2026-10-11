{% snapshot snp_rrr__customers %}

{{
    config(
      unique_key='customer_id',
      strategy='timestamp',
      updated_at='updated_at',
      invalidate_hard_deletes=True
    )
}}

select * from {{ ref('stg_rrr__customers') }}

{% endsnapshot %}

