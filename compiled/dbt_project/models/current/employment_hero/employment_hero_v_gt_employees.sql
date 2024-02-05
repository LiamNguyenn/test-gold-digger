
with 
  current_employment_history as (
    select * from "dev"."postgres_public"."employment_histories"
    where id in (
      select
        FIRST_VALUE(id) over (partition by member_id order by created_at desc rows between unbounded preceding and current row)
      from
        "dev"."postgres_public"."employment_histories"        
      where not _fivetran_deleted
      )
  )

select
  c.host_member_id
  , h.first_name + ' ' + h.last_name as full_name
  , h.organisation_id as host_organisation_id
  , ho.name as host_organisation_name
  , ho.country as host_organisation_country
--   , ee.name as host_org_other_name
  , p.organisation_id as peo_organisation_id
  , po.name as peo_organisation_name
  , replace(peo_organisation_name, 'Global Teams - ', '') as country
  , u.email
  , h.termination_date
  , h.active
--   , h.created_at as member_created_at
--   , h.start_date as member_start_date
  , h.global_teams_start_date
--   , c.updated_at
--   , c.created_at
  , c.host_termination_info
--   , c.status
  , eh.title
  , eh.employment_type
  , c.peo_member_id
from 
    "dev"."postgres_public"."peo_connections" c
    left join "dev"."postgres_public"."members" h 
        on c.host_member_id = h.id
        and not h._fivetran_deleted
        and not h.is_shadow_data
    left join "dev"."postgres_public"."users" u on 
        u.id = h.user_id
        and not u._fivetran_deleted
        and not u.is_shadow_data
    left join current_employment_history eh
        on c.host_member_id = eh.member_id        
    left join "dev"."postgres_public"."organisations" ho on 
        h.organisation_id = ho.id
        and not ho._fivetran_deleted
        and not ho.is_shadow_data
    left join "dev"."postgres_public"."members"  p on 
        c.peo_member_id = p.id
        and not p._fivetran_deleted
        and not p.is_shadow_data
    left join  "dev"."postgres_public"."organisations" po on 
        p.organisation_id = po.id
        and not po._fivetran_deleted
        and not po.is_shadow_data
where
  not c._fivetran_deleted
  and (
    u.email !~* '.*(employmenthero|employmentinnovations|keypay|webscale|thinkei|p2m|power2motivate|test|demo|exacc|sandbox|\\+).*'
 or 
    h.personal_email !~* '.*(employmenthero|employmentinnovations|keypay|webscale|thinkei|p2m|power2motivate|test|demo|exacc|sandbox|\\+).*'
 )
  and not peo_organisation_id in (143661,143663,143665,135211,133883,132979,126535,137662,133713,126540,10096,79759,8000,74240)
  and not host_organisation_id in (143661,143663,143665,135211,133883,132979,126535,137662,133713,126540,10096,79759,8000,74240)
  and host_organisation_id != 8701 -- adding 8701 here just in case we want to see GT employees attributed to EH