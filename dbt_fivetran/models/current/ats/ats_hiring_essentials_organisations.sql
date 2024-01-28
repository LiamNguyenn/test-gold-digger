{{ config(alias='hiring_essentials_organisations') }}

with
  org_creator as (
  select
    organisation_id
    , member_id
    , creator_email
  from
    (
      select
      m.organisation_id
      , m.id as member_id
      , u.email as creator_email
      , row_number() over (partition by m.organisation_id order by m.created_at) as rn
      from
        {{ source('postgres_public', 'members') }} as m
      join {{ source('postgres_public', 'users') }} as u on
        m.user_id = u.id
      where
        not m._fivetran_deleted        
        and not u._fivetran_deleted
        and not m.is_shadow_data
        and not u.is_shadow_data
      )
  where rn = 1 
    )

select 
  so.org_id_c as organisation_id, so.name, so.country_c as country
  ,o.created_at as organisation_created, so.created_date as created_on_sf
  ,so.active_employees_c as active_employees, o.estimated_number_of_employees
  ,so.subscription_plan_c as subscription_plan, so.lead_linking_id_c as lead_linking_id
  ,o.industry, o.sub_id, o.sub_name, o.pricing_tier, o.pricing_type
  ,m.creator_email
from 
    {{ source('salesforce', 'eh_org_c') }} so
  join {{ ref('employment_hero_organisations') }} o
    on so.org_id_c = o.id
    and o.id not in (select id from ats.spam_organisations) -- remove SPAM or Test organisations
  join org_creator as m on
    o.id = m.organisation_id 
where 
  so.ats_c 
  and not so._fivetran_deleted
  and not so.is_deleted
  and {{legit_emails('m.creator_email')}}
  and organisation_created::date>= '2023-04-12' -- launch of hiring essentials (previous ones are all test/old accounts)