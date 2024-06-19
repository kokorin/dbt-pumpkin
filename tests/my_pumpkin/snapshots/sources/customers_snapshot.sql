{% snapshot customers_snapshot %}

{{
    config(
      unique_key='id',
      strategy='check',
      check_cols='all',
    )
}}

select *
from {{ source('pumpkin', 'customers') }}

{% endsnapshot %}
