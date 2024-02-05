

--DEFINITION: 
-- 1. candidate :
--      a.cannot have an active member
--      b.must be verified
--      c.cannot have applied to jobs from spam orgs
--      d.length of country code cannot be 3 (created by country selector in the app not from swag jobs)

with
spam_jobs_created as (
    select j.id as job_id
    from
        "dev"."ats_public"."jobs" as j
    -- using this just to get the org id that created the job, no need to filter
    inner join "dev"."postgres_public"."organisations" as o
        on
            j.organisation_id = o.uuid
            and o.id in (select id from "dev"."ats"."spam_organisations") -- SPAM organisations
    where
        not j._fivetran_deleted
),

applicants_to_spam_jobs as (
    select
        cj.id,
        cj.job_id,
        cj.created_at           as applied_at,
        cj.source_name          as source,
        lower(cj.applied_email) as applicant_email,
        cj.user_id
    from
        "dev"."ats_public"."candidate_jobs" as cj
    inner join spam_jobs_created as j
        on
            cj.job_id = j.job_id
    where
        not cj._fivetran_deleted
),

swag_job_profiles as (
    select
        u.uuid                                      as user_uuid,
        u.id,
        lower(u.email)                              as email,
        u.created_at                                as user_created_at,
        u.updated_at                                as user_updated_at,
        ui.created_at,
        ui.updated_at,
        ui.first_name,
        ui.last_name,
        ui.user_verified_at,
        ui.source,
        ui.friendly_id,
        ui.completed_profile,
        ui.public_profile,
        ui.last_public_profile_at,
        ui.phone_number,
        ui.country_code,
        ui.state_code,
        a.city,
        g.latitude,
        g.longitude,
        ui.headline,
        ui.summary,
        ui.marketing_consented_at,
        count(case when m.active then m.id end)     as active_members,
        count(case when not m.active then m.id end) as terminated_members
    from
        "dev"."postgres_public"."users" as u
    inner join "dev"."postgres_public"."user_infos" as ui
        on
            u.id = ui.user_id
            and not ui._fivetran_deleted
    left join "dev"."postgres_public"."addresses" as a on ui.address_id = a.id and not a._fivetran_deleted
    left join 

(
select
    *
  from
    "dev"."postgres_public"."address_geolocations"
  where
    id in (
      select
        FIRST_VALUE(id) over(partition by address_id order by created_at desc rows between unbounded preceding and unbounded following)
      from
        "dev"."postgres_public"."address_geolocations"
      where
        not _fivetran_deleted
    )
)

 as g on a.id = g.address_id and not g._fivetran_deleted
    left join "dev"."postgres_public"."members" as m
        on
            u.id = m.user_id
            and not m._fivetran_deleted
            and not m.is_shadow_data
            and not m.system_user
            and not m.system_manager
    where
        
    u.email !~* '.*(employmenthero|employmentinnovations|keypay|webscale|thinkei|p2m|power2motivate|test|demo|exacc|sandbox|\\+).*'

        and not u._fivetran_deleted
        and not u.is_shadow_data
        and ui.user_verified_at is not NULL
        and (len(ui.country_code) is NULL or len(ui.country_code) != 3)
    group by 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24
    having active_members = 0
),

employment_history as (
    select
        user_id,
        count(*) as employment_entries
    from
        "dev"."postgres_public"."user_employment_histories"
    where
        not _fivetran_deleted
    group by 1
),

education_history as (
    select
        user_id,
        count(*) as education_entries
    from
        "dev"."postgres_public"."user_education_histories"
    where
        not _fivetran_deleted
    group by 1
),

resume_and_cover_letter as (
    select
        user_id,
        count(case when metadata ilike '%resume%' then 1 end)       as resume_entries,
        count(case when metadata ilike '%cover_letter%' then 1 end) as cover_letter_entries
    from
        "dev"."postgres_public"."user_attachments"
    where
        not _fivetran_deleted
    group by 1
)

select
    a.*,
    coalesce(employment_entries, 0)   as number_of_employment_entries,
    coalesce(education_entries, 0)    as number_of_education_entries,
    coalesce(resume_entries, 0)       as number_of_resume_entries,
    coalesce(cover_letter_entries, 0) as number_of_cover_letter_entries
from
    swag_job_profiles as a
left join employment_history as em
    on a.id = em.user_id
left join education_history as ed
    on a.id = ed.user_id
left join resume_and_cover_letter as r
    on a.id = r.user_id
where a.email not in (select distinct applicant_email from applicants_to_spam_jobs)