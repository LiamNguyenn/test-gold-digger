

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
    "dev"."postgres_public"."members" m  
    join "dev"."postgres_public"."users" u on u.id = m.user_id
    join "dev"."employment_hero"."organisations" o on o.id = m.organisation_id
    left join 

(
select
    *
  from
    "dev"."postgres_public"."employment_histories"
  where
    id in (
      select
        FIRST_VALUE(id) over(partition by member_id order by created_at desc rows between unbounded preceding and unbounded following)
      from
        "dev"."postgres_public"."employment_histories"
      where
        not _fivetran_deleted
    )
)

 h
      on m.id = h.member_id
where 
  
    email !~* '.*(employmenthero|employmentinnovations|keypay|webscale|thinkei|p2m|power2motivate|test|demo|exacc|sandbox|\\+).*'

  and not m.system_manager
  and not m.system_user
  and not m.independent_contractor
  and not m._fivetran_deleted
  and not u._fivetran_deleted
  and not m.is_shadow_data
  and not u.is_shadow_data
  and not m.dummy