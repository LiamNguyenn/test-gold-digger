{{ config(alias='job_application_profile') }}

with swag_job_profiles as (
    select 
      u.id, u.uuid as user_uuid, lower(u.email) as email, u.created_at as user_created_at
      , u.updated_at as user_updated_at, ui.created_at, ui.updated_at
      , ui.first_name, ui.last_name, ui.user_verified_at
      , ui.source, ui.friendly_id, ui.completed_profile
      , ui.public_profile, ui.last_public_profile_at
      , ui.phone_number, ui.country_code, ui.city, ui.state_code
      , ui.headline, ui.summary, ui.marketing_consented_at
    from 
      {{ source('postgres_public', 'users') }} u
      join {{ source('postgres_public', 'user_infos') }} ui on
        u.id = ui.user_id
        and not ui._fivetran_deleted
    where
      {{legit_emails('u.email')}}
      and not u._fivetran_deleted
      and not u.is_shadow_data
      and ui.user_verified_at is not null
      and (len(ui.country_code) is null or len(ui.country_code)!=3)
  )

  , employment_history as (
    select
      user_id      
      , count(*) as employment_entries
      , min(to_date(start_year || '-' || start_month || '-' || start_day , 'YYYY-MM-DD' )) as earliest_employment_start_date
    from
      {{ source('postgres_public', 'user_employment_histories') }}
    where
      not _fivetran_deleted
    group by 1
  )
  , education_history as (
    select
      user_id
      ,count(*) as education_entries
    from
        {{ source('postgres_public', 'user_education_histories') }}
    where
      not _fivetran_deleted
    group by 1
  )
  , resume_and_cover_letter as (
    select
      user_id
      , count(case when metadata ilike '%resume%' then 1 else null end) as resume_entries
      , count(case when metadata ilike '%cover_letter%' then 1 else null end) as cover_letter_entries
    from
        {{ source('postgres_public', 'user_attachments') }}
    where
      not _fivetran_deleted
    group by 1
  )
  
  , candidate_hiring_phases as (
    Select s.external_source_id as candidate_job_id
    , listagg(case when json_extract_path_text(c.content, 'activity_type') = 'move' then json_extract_path_text(c.content, 'full_message') else '' end, '; ') within group (order by c.created_at) as hiring_moves
    , listagg(json_extract_path_text(c.content, 'activity_type'), '; ') within group (order by c.created_at) as hiring_activities
    , count(*) as hiring_activity_count
    from {{source('comment_public', 'comments')}} c 
    join {{source('comment_public', 'comment_sources')}} s on c.comment_source_id = s.id
    join {{source('ats_public', 'candidate_jobs')}} cj on cj.id = s.external_source_id
    join {{source('ats_public', 'jobs')}} j on j.id = cj.job_id
    where not c._fivetran_deleted
    and not s._fivetran_deleted
    and not cj._fivetran_deleted
    and not j._fivetran_deleted
    and s.type = 'AtsJobCandidate'
    and is_valid_json(c.content)    
    group by 1
    )

select cj.id as candidate_job_id, u.user_uuid, u.country_code as candidate_country
, j.job_id, j.trim_job_title as job_title
, {{ job_title_without_seniority('trim_job_title') }} AS job_title_without_seniority
, {{ job_title_seniority_group('job_title_seniority') }} as job_title_seniority
, j.organisation_id, j.industry, j.country as job_country
, j.employment_type
, j.job_description
, j.candidate_location
, j.is_remote_job
, j.workplace_type
, cj.created_at as applied_at
, cj.contacted
  , case when cj.result = 1 then 'in progress'
      when cj.result = 2 then 'hired'
      else 'rejected' end as result
  , hp.name as current_hiring_phase
  , case when hp.phase_type = 0 then 'new'
      when hp.phase_type = 2 then 'in progress'
      when hp.phase_type = 1 then 'hiried'
      when hp.phase_type = 3 then 'rejected' end as hiring_phase_type
  , chp.hiring_moves
  , chp.hiring_activities
  , case when chp.hiring_activities ilike 'reject%' then true when chp.hiring_activities is not null and chp.hiring_activities != ''
 then false end as is_direct_reject
  , chp.hiring_activity_count
  , cj.user_id
  , lower(cj.applied_email) as applicant_email
  --, case when earliest_employment_start_date <= applied_at then em.employment_entries else null end as employment_entries
  , em.employment_entries
  , ed.education_entries
  , r.resume_entries
  , ars.score as affinda_score
  , JSON_EXTRACT_PATH_TEXT(JSON_EXTRACT_PATH_TEXT(ars.details, 'jobTitle'), 'score') as affinda_job_title_score
  , JSON_EXTRACT_PATH_TEXT(JSON_EXTRACT_PATH_TEXT(ars.details, 'location'), 'score') as affinda_location_score
  , JSON_EXTRACT_PATH_TEXT(JSON_EXTRACT_PATH_TEXT(ars.details, 'experience'), 'score') as affinda_experience_score
  , JSON_EXTRACT_PATH_TEXT(JSON_EXTRACT_PATH_TEXT(ars.details, 'managementLevel'), 'score') as affinda_management_level_score
  , ars.details as affinda_score_details
  --, ad.parse_data as resume_parse_data
  , mp.experience_job_titles
  , mp.applied_job_titles
  , mp.job_titles as all_job_titles
from  {{ source('ats_public', 'candidate_jobs') }} cj
join (select *, trim(job_title) as trim_job_title, lower({{ job_title_seniority('trim_job_title')}}) AS job_title_seniority from {{ ref('ats_jobs_created') }}) as j on cj.job_id = j.job_id 
join swag_job_profiles u on cj.user_id = u.user_uuid
join {{ref('employment_hero_organisations')}} o on o.id = j.organisation_id
left join candidate_hiring_phases chp on chp.candidate_job_id = cj.id
left join {{source('ats_public', 'hiring_phases')}} hp on cj.hiring_phase_id = hp.id and not hp._fivetran_deleted
left join employment_history em 
    on u.id = em.user_id
left join education_history as ed
    on u.id = ed.user_id
left join resume_and_cover_letter as r
    on u.id = r.user_id
left join {{current_row('ats_public', 'affinda_resume_scores', 'candidate_job_id')}} ars on ars.candidate_job_id = cj.id and not ars._fivetran_deleted
left join {{source('ats_public', 'affinda_documents')}} ad on ad.id = ars.candidate_job_document_id and not ad._fivetran_deleted
left join {{source('ats_public', 'matching_profiles')}} mp on mp.user_id = cj.user_id and not mp._fivetran_deleted
where 
  not cj._fivetran_deleted  
  and {{legit_emails('applicant_email')}}
--and cj.job_id in (select job_id from {{ref('ats_job_applications')}} where result = 'hired')
--and (em.employment_entries > 0)
and o.is_paying_eh
and j.job_title is not null and j.job_title !~ '^$' and len(j.job_title) !=1

--select hiring_phase, sum(case when affinda_score is not null then 1 else 0 end) as having_score, avg(affinda_score) as avg_affinda_score from staging_ats.job_application_profile group by 1  where employment_entries > 0 and affinda_score is not null group by 1
--select case when affinda_score < 0.25 then '<0.25' when affinda_score < 0.5 then '<0.5' when affinda_score < 0.75 then '<0.75' else '>= 0.75' end, count(affinda_score) from staging_ats.job_application_profile group by 1