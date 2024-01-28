{{ config(alias='bank_account', materialized = 'view') }}
select *
from {{ ref('int__keypay__bank_account') }}
