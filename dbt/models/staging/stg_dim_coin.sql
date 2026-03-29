select 
    coin_id,
    symbol
from {{ source('crypto', 'dim_coin') }}
