{{ config(alias='bank_payment_file_details', materialized = 'view') }}
select *
from {{ ref('int__keypay__bank_payment_file_details') }}
