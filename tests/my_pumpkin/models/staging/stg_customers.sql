select *
from {{ source('pumpkin', 'customers') }}
