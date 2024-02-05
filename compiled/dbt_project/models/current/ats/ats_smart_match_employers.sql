
-- to be added: Employer side member id

with shortlisted as (
    select
        o.uuid                                                                    as org_uuid,
        o.id                                                                      as org_id,
        cj.user_id                                                                as candidate_user_uuid,
        cj.job_id,
        j.job_title
        ,  
case
    when regexp_replace(job_title, '^(Deputy |Casual )', '', 1, 'i') ~* 'assistant accountant' 
        then INITCAP(trim(regexp_replace(regexp_replace(job_title, '^(Deputy |Casual )', '', 1, 'i'), '^(Graduate |Trainee |Associate |Apprentice |Junior |Intermediate |Senior |Lead |Principal |Chief |Head |Executive |Vice |Managing )', '', 1, 'i'))) 
    when regexp_replace(job_title, '^(Deputy |Casual )', '', 1, 'i') !~* '^(Graduate |Associate |Assistant |Apprentice |Junior |Intermediate |Senior |Lead |Principal |Chief |Head |Executive |Vice |Managing )(of |to |\or |\and )'
        and regexp_replace(job_title, '^(Deputy |Casual )', '', 1, 'i') !~* '^(Chief |Executive |Lead ).*(officer|assistant|generator).*'
    then INITCAP(trim(regexp_replace(regexp_replace(job_title, '^(Deputy |Casual )', '', 1, 'i'), '^(Graduate |Trainee |Associate |Assistant |Apprentice |Junior |Intermediate |Senior |Lead |Principal |Chief |Head |Executive |Vice |Managing )', '', 1, 'i'))) 
    else INITCAP(regexp_replace(job_title, '^(Deputy |Casual )', '', 1, 'i')) end
 as job_title_without_seniority
        ,  
case 
        when INITCAP(job_title_seniority) in ('Associate', 'Assistant', 'Graduate', 'Apprentice', 'Trainee') then 'Junior'
        when INITCAP(job_title_seniority) = '' or INITCAP(job_title_seniority) is null then 'Intermediate'
        when INITCAP(job_title_seniority) in ('Principal', 'Leader') then 'Lead'
        when INITCAP(job_title_seniority) in ('Managing') then 'Manager'
        when INITCAP(job_title_seniority) in ('Head') then 'Head'
        when INITCAP(job_title_seniority) in ('Vice', 'Executive') then 'Director'
        else INITCAP(job_title_seniority) end 
 as job_title_seniority,
        j.job_sector,
        j.industry,
        j.country,
        j.employment_type,
        j.job_description,
        j.candidate_location,
        j.is_remote_job,
        j.workplace_type,
        cj.created_at,
        cj._fivetran_deleted,
        cj.id
    from "dev"."ats_public"."candidate_jobs" as cj
    inner join (select
        *,
        lower( 
case when regexp_replace(job_title, '^(Deputy |Casual )', '', 1, 'i') ~ '^(Apprentice |Graduate |Trainee |Junior |Intermediate |Senior |Managing |Lead |Leader |Head |Vice |Manager |Director |Chief )' 
        then trim(regexp_substr(regexp_replace(job_title, '^(Deputy |Casual )', '', 1, 'i'), '^(Apprentice |Graduate |Trainee |Junior |Intermediate |Senior |Managing |Lead |Leader |Head |Vice |Manager |Director |Chief )', 1, 1, 'i'))        
    when regexp_replace(job_title, '^(Deputy |Casual )', '', 1, 'i') !~* '^(Associate |Assistant |Principal |Executive )(of |to )'
        and trim(regexp_substr(regexp_replace(job_title, '^(Deputy |Casual )', '', 1, 'i'), '^(Associate |Assistant |Principal |Executive )', 1, 1, 'i')) != ''
        then trim(regexp_substr(regexp_replace(job_title, '^(Deputy |Casual )', '', 1, 'i'), '^(Associate |Assistant |Principal |Executive )', 1, 1, 'i'))
    when job_title ~ '(^|\\W)Apprentice(\\W|$)' then 'Apprentice'
    when job_title ~ '(^|\\W)Graduate(\\W|$)' then 'Graduate'
    when job_title ~ '(^|\\W)Junior(\\W|$)' then 'Junior'
    when job_title ~ '(^|\\W)Intermediate(\\W|$)' then 'Intermediate'
    when job_title ~ '(^|\\W)Senior(\\W|$)' then 'Senior'    
    when job_title ~ '(^|\\W)Managing(\\W|$)' then 'Managing'
    when job_title ~ '(^|\\W)(Lead|Leader)(\\W|$)' then 'Lead'
    when job_title ~ '(^|\\W)Trainee(\\W|$)' then 'Trainee'
    when job_title ~ '(^|\\W)Head(\\W|$)' then 'Head'
    when job_title ~ '(^|\\W)Vice(\\W|$)' then 'Vice'
    when job_title ~ '(^|\\W)Manager(\\W|$)' then 'Manager'
    when job_title ~ '(^|\\W)Director(\\W|$)' then 'Director'
    when job_title ~ '(^|\\W)Chief(\\W|$)' then 'Chief'
    else null end
) as job_title_seniority
    from "dev"."ats"."jobs_created") as j on cj.job_id = j.job_id
    inner join "dev"."employment_hero"."organisations" as o on j.organisation_id = o.id
    where
        cj.source_name = 'Shortlisted from Saved candidate'
        and not j.is_test_job
        and is_paying_eh
),

saved as (
    select distinct
        o.uuid                                                                                                                                                           as org_uuid,
        o.id                                                                                                                                                             as org_id,
        u.uuid                                                                                                                                                           as candidate_user_uuid,
        sc.job_saved_for,
        sc.created_at,
        o.industry,
        o.country,
        sc.id,
        sc._fivetran_deleted,
        first_value(eh.title) over (partition by sc._fivetran_deleted, eh.member_id order by eh.start_date asc rows between unbounded preceding and unbounded following) as employer_job_title
    from "dev"."postgres_public"."saved_candidates" as sc
    inner join "dev"."employment_hero"."organisations" as o on sc.organisation_id = o.id
    inner join "dev"."postgres_public"."users" as u on sc.user_id = u.id
    left join "dev"."employment_hero"."employees" as he on sc.author_id = he.id
    left join "dev"."postgres_public"."employment_histories" as eh on he.id = eh.member_id and sc.created_at > eh.start_date and not eh._fivetran_deleted
    where o.is_paying_eh
)

select
    coalesce(l.org_uuid, s.org_uuid)                       as org_uuid,
    coalesce(l.org_id, s.org_id)                           as org_id,
    coalesce(l.candidate_user_uuid, s.candidate_user_uuid) as candidate_user_uuid,
    coalesce(l.country, s.country)                         as country,
    s.employer_job_title,
    s.job_saved_for,
    s.created_at                                           as saved_at,
    l.job_title                                            as shortlisted_job_title,
    l.created_at                                           as shortlisted_at,
    l.job_title_without_seniority,
    l.job_title_seniority,
    l.job_id,
    l.job_sector,
    l.industry,
    l.employment_type,
    l.job_description,
    l.candidate_location,
    l.is_remote_job,
    l.workplace_type
from (
    select * from shortlisted
    where id in (
        select first_value(id) over (partition by org_uuid, candidate_user_uuid order by _fivetran_deleted asc, created_at desc rows between unbounded preceding and unbounded following)
        from
            shortlisted
    )
) as l
full outer join
    (
        select * from saved
        where id in (
            select first_value(id) over (partition by org_uuid, candidate_user_uuid order by _fivetran_deleted asc, created_at desc rows between unbounded preceding and unbounded following)
            from saved
        )
    ) as s on l.org_uuid = s.org_uuid and l.candidate_user_uuid = s.candidate_user_uuid