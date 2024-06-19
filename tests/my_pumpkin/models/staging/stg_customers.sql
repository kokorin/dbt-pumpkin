select id, name
from {{ ref('customers_snapshot') }}
where dbt_valid_to is null
