{{
    config(
        materialized='incremental',
        alias='daily_members'
    )
}}

select *
from
(
  select
	distinct date_trunc('day',e.timestamp) date
    , {{ try_cast("user_id", 'int') }} as user_id
    , {{ try_cast("member_id", 'int') }} as member_id
from {{ ref('customers_int_events') }} e
    where e.timestamp < (select date_trunc('day', max("timestamp")) from {{ ref('customers_int_events') }})
    
{% if is_incremental() %}
    and date_trunc('day', e.timestamp) > (select max(date) from {{ this }} )
{% endif %}

)
where user_id is not null