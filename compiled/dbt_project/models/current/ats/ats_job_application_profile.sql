

with swag_job_profiles as (
    select
        u.id,
        u.uuid         as user_uuid,
        lower(u.email) as email,
        u.created_at   as user_created_at,
        u.updated_at   as user_updated_at,
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
        ui.city,
        ui.state_code,
        ui.headline,
        ui.summary,
        ui.marketing_consented_at
    from
        "dev"."postgres_public"."users" as u
    inner join "dev"."postgres_public"."user_infos" as ui
        on
            u.id = ui.user_id
            and not ui._fivetran_deleted
    where
        
    u.email !~* '.*(employmenthero|employmentinnovations|keypay|webscale|thinkei|p2m|power2motivate|test|demo|exacc|sandbox|\\+).*'

        and not u._fivetran_deleted
        and not u.is_shadow_data
        and ui.user_verified_at is not NULL
        and (len(ui.country_code) is NULL or len(ui.country_code) != 3)
),

employment_history as (
    select
        user_id,
        count(*)                                                                         as employment_entries,
        min(to_date(start_year || '-' || start_month || '-' || start_day, 'YYYY-MM-DD')) as earliest_employment_start_date
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
),

candidate_hiring_phases as (
    select
        s.external_source_id                                                                                                                                                                         as candidate_job_id,
        listagg(case when json_extract_path_text(c.content, 'activity_type') = 'move' then json_extract_path_text(c.content, 'full_message') else '' end, '; ') within group (order by c.created_at) as hiring_moves,
        listagg(json_extract_path_text(c.content, 'activity_type'), '; ') within group (order by c.created_at)                                                                                       as hiring_activities,
        count(*)                                                                                                                                                                                     as hiring_activity_count
    from "dev"."comment_public"."comments" as c
    inner join "dev"."comment_public"."comment_sources" as s on c.comment_source_id = s.id
    inner join "dev"."ats_public"."candidate_jobs" as cj on s.external_source_id = cj.id
    inner join "dev"."ats_public"."jobs" as j on cj.job_id = j.id
    where
        not c._fivetran_deleted
        and not s._fivetran_deleted
        and not cj._fivetran_deleted
        and not j._fivetran_deleted
        and s.type = 'AtsJobCandidate'
        and is_valid_json(c.content)
    group by 1
)

select
    cj.id                                                                                   as candidate_job_id,
    u.user_uuid,
    u.country_code                                                                          as candidate_country,
    j.job_id,
    j.trim_job_title                                                                        as job_title
    ,  
case
    when regexp_replace(trim_job_title, '^(Deputy |Casual )', '', 1, 'i') ~* 'assistant accountant' 
        then INITCAP(trim(regexp_replace(regexp_replace(trim_job_title, '^(Deputy |Casual )', '', 1, 'i'), '^(Graduate |Trainee |Associate |Apprentice |Junior |Intermediate |Senior |Lead |Principal |Chief |Head |Executive |Vice |Managing )', '', 1, 'i'))) 
    when regexp_replace(trim_job_title, '^(Deputy |Casual )', '', 1, 'i') !~* '^(Graduate |Associate |Assistant |Apprentice |Junior |Intermediate |Senior |Lead |Principal |Chief |Head |Executive |Vice |Managing )(of |to |\or |\and )'
        and regexp_replace(trim_job_title, '^(Deputy |Casual )', '', 1, 'i') !~* '^(Chief |Executive |Lead ).*(officer|assistant|generator).*'
    then INITCAP(trim(regexp_replace(regexp_replace(trim_job_title, '^(Deputy |Casual )', '', 1, 'i'), '^(Graduate |Trainee |Associate |Assistant |Apprentice |Junior |Intermediate |Senior |Lead |Principal |Chief |Head |Executive |Vice |Managing )', '', 1, 'i'))) 
    else INITCAP(regexp_replace(trim_job_title, '^(Deputy |Casual )', '', 1, 'i')) end
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
    j.organisation_id,
    j.industry,
    j.country                                                                               as job_country,
    j.employment_type,
    j.job_description,
    j.candidate_location,
    j.is_remote_job,
    j.workplace_type,
    cj.created_at                                                                           as applied_at,
    cj.contacted,
    case
        when cj.result = 1 then 'in progress'
        when cj.result = 2 then 'hired'
        else 'rejected'
    end                                                                                     as result,
    hp.name                                                                                 as current_hiring_phase,
    case
        when hp.phase_type = 0 then 'new'
        when hp.phase_type = 2 then 'in progress'
        when hp.phase_type = 1 then 'hiried'
        when hp.phase_type = 3 then 'rejected'
    end                                                                                     as hiring_phase_type,
    chp.hiring_moves,
    chp.hiring_activities,
    case
        when chp.hiring_activities ilike 'reject%' then TRUE when chp.hiring_activities is not NULL and chp.hiring_activities != ''
            then FALSE
    end                                                                                     as is_direct_reject,
    chp.hiring_activity_count,
    cj.user_id,
    lower(cj.applied_email)                                                                 as applicant_email,
    --, case when earliest_employment_start_date <= applied_at then em.employment_entries else null end as employment_entries
    em.employment_entries,
    ed.education_entries,
    r.resume_entries,
    ars.score                                                                               as affinda_score,
    json_extract_path_text(json_extract_path_text(ars.details, 'jobTitle'), 'score')        as affinda_job_title_score,
    json_extract_path_text(json_extract_path_text(ars.details, 'location'), 'score')        as affinda_location_score,
    json_extract_path_text(json_extract_path_text(ars.details, 'experience'), 'score')      as affinda_experience_score,
    json_extract_path_text(json_extract_path_text(ars.details, 'managementLevel'), 'score') as affinda_management_level_score,
    ars.details                                                                             as affinda_score_details,
    --, ad.parse_data as resume_parse_data
    mp.experience_job_titles,
    mp.applied_job_titles,
    mp.job_titles                                                                           as all_job_titles
from "dev"."ats_public"."candidate_jobs" as cj
inner join (select
    *,
    trim(job_title) as trim_job_title,
    lower( 
case when regexp_replace(trim_job_title, '^(Deputy |Casual )', '', 1, 'i') ~ '^(Apprentice |Graduate |Trainee |Junior |Intermediate |Senior |Managing |Lead |Leader |Head |Vice |Manager |Director |Chief )' 
        then trim(regexp_substr(regexp_replace(trim_job_title, '^(Deputy |Casual )', '', 1, 'i'), '^(Apprentice |Graduate |Trainee |Junior |Intermediate |Senior |Managing |Lead |Leader |Head |Vice |Manager |Director |Chief )', 1, 1, 'i'))        
    when regexp_replace(trim_job_title, '^(Deputy |Casual )', '', 1, 'i') !~* '^(Associate |Assistant |Principal |Executive )(of |to )'
        and trim(regexp_substr(regexp_replace(trim_job_title, '^(Deputy |Casual )', '', 1, 'i'), '^(Associate |Assistant |Principal |Executive )', 1, 1, 'i')) != ''
        then trim(regexp_substr(regexp_replace(trim_job_title, '^(Deputy |Casual )', '', 1, 'i'), '^(Associate |Assistant |Principal |Executive )', 1, 1, 'i'))
    when trim_job_title ~ '(^|\\W)Apprentice(\\W|$)' then 'Apprentice'
    when trim_job_title ~ '(^|\\W)Graduate(\\W|$)' then 'Graduate'
    when trim_job_title ~ '(^|\\W)Junior(\\W|$)' then 'Junior'
    when trim_job_title ~ '(^|\\W)Intermediate(\\W|$)' then 'Intermediate'
    when trim_job_title ~ '(^|\\W)Senior(\\W|$)' then 'Senior'    
    when trim_job_title ~ '(^|\\W)Managing(\\W|$)' then 'Managing'
    when trim_job_title ~ '(^|\\W)(Lead|Leader)(\\W|$)' then 'Lead'
    when trim_job_title ~ '(^|\\W)Trainee(\\W|$)' then 'Trainee'
    when trim_job_title ~ '(^|\\W)Head(\\W|$)' then 'Head'
    when trim_job_title ~ '(^|\\W)Vice(\\W|$)' then 'Vice'
    when trim_job_title ~ '(^|\\W)Manager(\\W|$)' then 'Manager'
    when trim_job_title ~ '(^|\\W)Director(\\W|$)' then 'Director'
    when trim_job_title ~ '(^|\\W)Chief(\\W|$)' then 'Chief'
    else null end
)                   as job_title_seniority
from "dev"."ats"."jobs_created") as j on cj.job_id = j.job_id
inner join swag_job_profiles as u on cj.user_id = u.user_uuid
inner join "dev"."employment_hero"."organisations" as o on j.organisation_id = o.id
left join candidate_hiring_phases as chp on cj.id = chp.candidate_job_id
left join "dev"."ats_public"."hiring_phases" as hp on cj.hiring_phase_id = hp.id and not hp._fivetran_deleted
left join employment_history as em
    on u.id = em.user_id
left join education_history as ed
    on u.id = ed.user_id
left join resume_and_cover_letter as r
    on u.id = r.user_id
left join 

(
select
    *
  from
    "dev"."ats_public"."affinda_resume_scores"
  where
    id in (
      select
        FIRST_VALUE(id) over(partition by candidate_job_id order by created_at desc rows between unbounded preceding and unbounded following)
      from
        "dev"."ats_public"."affinda_resume_scores"
      where
        not _fivetran_deleted
    )
)

 as ars on cj.id = ars.candidate_job_id and not ars._fivetran_deleted
left join "dev"."ats_public"."affinda_documents" as ad on ars.candidate_job_document_id = ad.id and not ad._fivetran_deleted
left join "dev"."ats_public"."matching_profiles" as mp on cj.user_id = mp.user_id and not mp._fivetran_deleted
where
    not cj._fivetran_deleted
    and 
    applicant_email !~* '.*(employmenthero|employmentinnovations|keypay|webscale|thinkei|p2m|power2motivate|test|demo|exacc|sandbox|\\+).*'

    --and cj.job_id in (select job_id from "dev"."ats"."job_applications" where result = 'hired')
    --and (em.employment_entries > 0)
    and o.is_paying_eh
    and j.job_title is not NULL and j.job_title !~ '^$' and len(j.job_title) != 1

--select hiring_phase, sum(case when affinda_score is not null then 1 else 0 end) as having_score, avg(affinda_score) as avg_affinda_score from staging_ats.job_application_profile group by 1  where employment_entries > 0 and affinda_score is not null group by 1
--select case when affinda_score < 0.25 then '<0.25' when affinda_score < 0.5 then '<0.5' when affinda_score < 0.75 then '<0.75' else '>= 0.75' end, count(affinda_score) from staging_ats.job_application_profile group by 1