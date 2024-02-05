

with keypay as (
    select b.id
    , b.name
    , b.created_at
    , coalesce(case when b.industry_id is null and b.industry_name is not null then 'Other' else i.name  end
            , case when z.primary_industry is not null and z.primary_industry != '' then z.primary_industry else null end) as industry
    , b.country as country
    , b.commence_billing_from::date
    , billing.last_billing_month
from
    "dev"."keypay"."business_traits" b    
    left join "dev"."keypay"."industry" i on b.industry_id = i.id
    left join "dev"."keypay"."zoom_info" z on b.id = z._id
    left join (
            select business_id, max(billing_month::date) as last_billing_month from "dev"."keypay"."_t_pay_run_total_monthly_summary" group by 1
        )billing on billing.business_id = b.id      
     )

select
    CONCAT(case when o.id is not null then 'EH-' || o.id else '' end, case when o.id is not null and b.id is not null then '_KP-' || b.id when b.id is not null then 'KP-' || b.id else '' end) as omop_org_id
    , o.id as eh_organisation_id
    , b.id as kp_business_id    
    , coalesce(o.name, b.name) as name
    , least(o.created_at, b.created_at) as created_at
    , coalesce(o.country, b.country) as country
    , case 
      when coalesce(o.industry, b.industry) = 'Other' then 'Other'
      when coalesce(o.industry, b.industry) != 'Other' and coalesce(o.industry, b.industry) is not null then i.consolidated_industry 
      else null
    end as industry
    , o.sub_name as eh_sub_name    
    , o.pricing_tier as eh_pricing_tier
    , o.is_paying_eh
    , o.churn_date as eh_churn_date    
    , b.commence_billing_from::date as kp_commence_billing_from    
    , b.last_billing_month as kp_last_billing_month
from
  "dev"."employment_hero"."organisations" o   
  full outer join keypay b on o.external_id = b.id and o.payroll_type = 'Keypay'   
  left join "dev"."one_platform"."industry" as i on 
    regexp_replace( coalesce(o.industry, b.industry),'\\s','') = regexp_replace( i.eh_industry,'\\s','')
    or regexp_replace( coalesce(o.industry, b.industry),'\\s','') = regexp_replace( i.keypay_industry,'\\s','')
    or regexp_replace( coalesce(o.industry, b.industry),'\\s','') = regexp_replace( i.zoom_info_primary_industry,'\\s','')