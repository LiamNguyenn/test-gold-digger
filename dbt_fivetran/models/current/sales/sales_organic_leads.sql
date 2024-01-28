{{ config(alias='organic_leads') }}

with  
  converted as (
    select
      organisation_id      
      , converted_at
      , pricing_tier as first_paid_tier
    from (
        select a.organisation_id      
        , a.created_at as converted_at
        , s.pricing_tier 
        , row_number() over (partition by a.organisation_id ORDER BY a.created_at) rn       
        from 
          postgres_public.agreements a 
          join {{ref('employment_hero_v_sub_plan_grouping')}} as s on a.subscription_plan_id = s.id
        where
          not a._fivetran_deleted
          and s.pricing_tier != 'free'        
      )
    where rn = 1
  )
  , salesforce_eh_orgs as (
    select
      eh_org_c.id as sf_eh_org_id
      , o.*
    from
      salesforce.eh_org_c
      join employment_hero.organisations as o on eh_org_c.org_id_c = o.id
    where
      not eh_org_c.is_deleted
  )
  , employees as (
    select
      m.email
      ,m.organisation_id as org_id
      ,m.org_name
      ,m.sub_name
      ,o.created_at as org_created_at
    from
      employment_hero.employees m
      join postgres_public.organisations o on m.organisation_id = o.id and not o._fivetran_deleted and not o.is_shadow_data
  )


select distinct
  l.id as lead_id
  , l.created_date as lead_created_date
  , l.status as lead_status
  , l.company as lead_company
  , coalesce(l.industry_primary_c, 'Unknown') as lead_industry
  , l.number_of_employees as lead_num_of_employees
  , l.country as lead_country
  , l.name as lead_name
  , l.email as lead_email
  , l.title as lead_title
  , l.lost_reason_c as lead_lost_reason
  , l.lost_sub_reason_c as lead_lost_sub_reason
  , l.lost_reason_detail_c as lead_lost_reason_detail
  , l.most_recent_conversion_c as campaign
  , l.date_assigned_to_owner_c as date_assigned_to_owner
  , l.most_recent_sales_contact_c as most_recent_sales_contact
  , coalesce(o.id, m.org_id) as organisation_id
  , case
      when o.id is not null then o.name
      when o.id is null and m.org_id is not null then m.org_name
      else null
    end as eh_organisation_name
  , case
      when o.id is not null then o.created_at
      when o.id is null and m.org_id is not null then m.org_created_at
      else null
    end as opportunity_date
  , case
      when o.id is not null then o.sub_name
      when o.id is null and m.org_id is not null then m.sub_name
      else null
    end as eh_subscription
  , c.converted_at as conversion_date
  , case when eh_subscription ilike '%CSA%' or eh_subscription ilike '%Reseller%' or lead_lost_reason ilike '%Unqualified%' or lead_lost_reason ilike 'Authority' then true else false end as unqualified
  , case when eh_subscription ilike '%free%' then true else false end as opportunity
  , case when eh_subscription ilike '%Zuora%' then true else false end as close_won
  , case when eh_subscription is null then true else false end as bad_lead
  
from 
  salesforce.lead as l
  -- owner must be for Organics, currently only Wylie, l.owner_id = '0055h000000ae0iAAA'
  join salesforce.user as u on l.owner_id = u.id and u.market_c = 'Organic' and not u._fivetran_deleted
  -- if eh_org exist in salesforce, use that as it will be accurate but since its not governed, many are null
  -- so if they are null then join on the email to get the org, not the most accurate but its the only other link from leads to orgs
  left join salesforce_eh_orgs as o on o.sf_eh_org_id = l.eh_org_c and (o.sub_name != 'Demo' or o.sub_name is null)
  left join employees as m on m.email = l.email and (m.sub_name != 'Demo' or m.sub_name is null)
  left join converted as c on coalesce(o.id, m.org_id) = c.organisation_id
where
  l.email !~* '.*(employmenthero|employmentinnovations|thinkei|p2m|power2motivate|test|demo|exacc|sandbox|\\+).*'
  and l.number_of_employees <= 10
  and l.created_date > '2021-03-31'
order by lead_created_date desc