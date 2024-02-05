

select distinct
    cj.id,
    cj.job_id,
    j.job_title,
    j.created_at                                                 as job_created_at,
    j.job_sector,
    j.organisation_id,
    j.industry,
    j.country,
    j.is_remote_job,
    j.is_test_job,
    case
        when cj.result = 1
            then 'in progress'
        when cj.result = 2
            then 'hired'
        else 'rejected'
    end                                                          as result,
    cj.created_at                                                as applied_at,
    cj.updated_at,
    cj.hired_at,
    datediff(days, cj.created_at, cj.hired_at)                   as time_to_hire,
    coalesce(
        portal.name,
        case
            when cj.source_name ~* '.*(direct).*' then 'Employment Hero Careers'
            when cj.source_name ~* '.*(jora).*' then 'JORA'
            when cj.source_name ~* '.*(adzuna).*' then 'Adzuna'
            when cj.source_name ~* '.*(seek).*' then 'SEEK'
        end
    )                                                            as portal_name,
    coalesce(posting.posted_at, cpj.created_at, jbja.created_at) as posted_at,
    cj.source_name                                               as source,
    case
        when cj.source_name ~* '.*(manual|shortlisted|copied).*' then 'Manual'
        when cj.source_name ~* '.*(direct|jora|adzuna|referral).*' then 'Direct'
        else 'Indirect'
    end                                                          as application_type,
    cj.applied_first_name || ' ' || cj.applied_last_name         as applicant,
    lower(cj.applied_email)                                      as applicant_email,
    cj.user_id
from
    "dev"."ats_public"."candidate_jobs" as cj
inner join "dev"."ats"."jobs_created" as j
    on
        cj.job_id = j.job_id
left join "dev"."ats_public"."job_boards_postings" as posting
    on
        cj.vendor_posting_id = posting.vendor_posting_id
        and not posting._fivetran_deleted
left join "dev"."ats_public"."job_boards_portals" as portal
    on
        posting.job_boards_portal_id = portal.id
        and not portal._fivetran_deleted
left join "dev"."ats_public"."career_page_jobs" as cpj
    on
        cj.career_page_job_id = cpj.id
left join "dev"."ats_public"."job_boards_job_adverts" as jbja
    on
        cj.job_boards_job_advert_id = jbja.id
where
    not cj._fivetran_deleted
    and 
    applicant_email !~* '.*(employmenthero|employmentinnovations|keypay|webscale|thinkei|p2m|power2motivate|test|demo|exacc|sandbox|\\+).*'
