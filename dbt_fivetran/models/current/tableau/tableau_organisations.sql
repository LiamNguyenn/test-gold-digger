select distinct
    o.id eh_org_id,
    o.industry,
    o.name org_name,
    o.zuora_account_id,
    cast(o.created_at as date) eh_org_created_date,
    o.payroll_type,
    o.connected_app,
    o.is_paying_eh,
    o.pricing_tier,
    o.pricing_type,
    o.currency org_currency,
    cast(o1.eh_churn_date as date) eh_churn_date,
    o1.kp_business_id,
    o1.country,
    o1.kp_commence_billing_from,
    o1.kp_last_billing_month
from {{ ref("employment_hero_organisations") }} o
left join {{ ref("one_platform_organisations") }} o1 on o1.eh_organisation_id = o.id
where o.is_demo = 'f' and o.is_shadow_data = 'f' and o._fivetran_deleted = 'f'
