{{ config(alias='opportunities') }}

select
    opportunity.id,
    opportunity.name,
    opportunity.created_date,
    owner.name as owner,
    demo_sat_date_c as demo_sat_date,
    geo_code_c as geo_code,
    opportunity_originator_market_c as originator_market,
    stage_name,
    lost_reason_c as lost_reason,
    lost_sub_reason_c as lost_sub_reason,
    originator.name as originator,
    record_type.name as record_type,
    opportunity.existing_customer_revenue_type_c as existing_customer_revenue_type
from salesforce.opportunity
join salesforce.account on opportunity.account_id = account.id
join salesforce.user as owner on opportunity.owner_id = owner.id
join
    salesforce.user as originator
    on opportunity.opportunity_originator_c = originator.id
join salesforce.record_type on opportunity.record_type_id = record_type.id
where
    not opportunity.is_deleted
    and not opportunity._fivetran_deleted
    and opportunity.name not ilike '%test%'
    and not account.is_deleted
    and not account._fivetran_deleted
    and account.name not ilike '%test%'
