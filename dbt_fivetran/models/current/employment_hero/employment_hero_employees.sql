{{ config(alias='employees') }}

select 
  m.*,
  u.uuid as user_uuid,
  u.email,
--   a.state as state,
--   a.city as suburb,
--   a.postcode,
--   case
--       when a.state ~* '^[\\W]?nsw|act|^[\\W]?vic|^nt|qld|^s[.]?a[.]?$|tas|new south wales|sydney|^w[.]?a$|western aus|south australia|queensland|northern territory|australia' then 'AU'
--       else a.country
--   end as country,
--   loc.country as working_country,
  h.title as latest_job_title,
  h.industry_standard_job_title,
  h.employment_type as latest_employment_type,
  o.name as org_name,
  o.sub_name
from
    {{source('postgres_public', 'members')}} m  
    join {{source('postgres_public', 'users')}} u on u.id = m.user_id
    join {{ref('employment_hero_organisations')}} o on o.id = m.organisation_id
    left join {{ current_row('postgres_public', 'employment_histories', 'member_id') }} h
      on m.id = h.member_id
  -- left join postgres_public.addresses a on
  --   m.address_id = a.id
  -- left join postgres_public.work_locations as loc on
  --   m.work_location_id = loc.id
where 
  {{legit_emails('email')}}
  and not m.system_manager
  and not m.system_user
  and not m.independent_contractor
  and not m._fivetran_deleted
  and not u._fivetran_deleted
  and not m.is_shadow_data
  and not u.is_shadow_data
  and not m.dummy