{{ config(
    materialized = 'incremental',
    unique_key = 'date',
    alias='instapay_members_aggregation'
) }}


select 
    getdate()::date as date
    , count(member_id) as instapay_members
    , count(case when first_time_swag_app is not null then 1 end) as instapay_members_using_swagapp
from 
    {{ref('ebenefits_instapay_eligible_member_profile')}}
{% if is_incremental() %}
    where date >= (select max(date) from {{ this }})
{% endif %}