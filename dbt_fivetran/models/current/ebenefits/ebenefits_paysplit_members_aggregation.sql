{{ config(
    materialized = 'incremental',
    unique_key = 'date',
    alias='paysplit_members_aggregation'
) }}


select 
    getdate()::date as date
    , count(member_id) as paysplit_members
    , count(case when has_wallet_account then 1 end) as paysplit_members_with_wallet
from 
    {{ ref('ebenefits_paysplit_members') }}
{% if is_incremental() %}
    where date >= (select max(date) from {{ this }})
{% endif %}