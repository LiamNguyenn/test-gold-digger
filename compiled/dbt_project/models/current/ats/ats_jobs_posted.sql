

select
    posting.job_id,
    j.created_at  as job_created_at,
    j.job_title,
    j.job_sector,
    j.organisation_id,
    o.name        as organisation_name,
    o.pricing_tier,
    j.industry,
    j.country,
    posting.id    as job_boards_post_id,
    portal.name   as portal_name,
    portal.source as portal_source,
    posting.posted_at,
    posting.created_at,
    posting.updated_at,
    posting.expired_at,
    j.job_status,
    case
        when status = 1
            then 'pending'
        when status = 2
            then 'posted'
        when status = 3
            then 'updated'
        when status = 4
            then 'failed'
        when status = 5
            then 'expired'
        else 'deleted'
    end           as job_posting_status,
    posting.applicants_count,
    j.is_remote_job,
    j.is_test_job
from
    "dev"."ats_public"."job_boards_postings" as posting
inner join "dev"."ats"."jobs_created" as j
    on
        posting.job_id = j.job_id
inner join "dev"."ats_public"."job_boards_portals" as portal
    on
        posting.job_boards_portal_id = portal.id
left join "dev"."employment_hero"."organisations" as o on j.organisation_id = o.id
where
    not posting._fivetran_deleted