{{ config(alias='reseller_contact', materialized = 'view') }}
select *
from {{ ref('int__keypay__reseller_contact') }}
