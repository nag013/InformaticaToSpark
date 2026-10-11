{% macro stable_key(columns) -%}
    md5(
        {%- for column in columns -%}
        coalesce(cast({{ column }} as string), '_dbt_null_')
        {%- if not loop.last %} || '|' || {% endif -%}
        {%- endfor -%}
    )
{%- endmacro %}

