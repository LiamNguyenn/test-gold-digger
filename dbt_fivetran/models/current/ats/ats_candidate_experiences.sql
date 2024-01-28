{{ config(alias='candidate_experiences') }}

with 
  candidate_experience as (
    select id
      , user_id
      , industry_standard_job_title
      , trim(job_title) as trim_job_title
      , {{ job_title_without_seniority('trim_job_title') }} AS job_title_without_seniority
      , lower({{ job_title_seniority('trim_job_title')}}) AS job_title_seniority
      , company
      , summary
      , current_job
      , case when coalesce(start_year, end_year) < 1900 or coalesce(start_year, end_year) > 2100 then null else to_date(coalesce(start_year, end_year) || '-' || coalesce(case when start_month > 12 or start_month < 1 then null else start_month end, 6) || '-' || coalesce(start_day, 1), 'YYYY-MM-DD') end as start_date
      , case when coalesce(end_year, start_year) < 1900 or coalesce(end_year, start_year) > 2100 then null else to_date(coalesce(end_year, start_year) || '-' || coalesce(case when end_month > 12 or start_month < 1 then null else end_month end, 7) || '-' || coalesce(end_day, 1) , 'YYYY-MM-DD') end as end_date
      , case when current_job then 0 else greatest(0, datediff('month', least(coalesce(end_date, current_date), current_date), current_date)::float/12) end as gap_years_to_date
      , greatest(0, datediff('month', least(coalesce(start_date, end_date), current_date), least(coalesce(end_date, current_date), current_date))::float/12) as duration_years
      from
        {{ source('postgres_public', 'user_employment_histories') }}
      where not _fivetran_deleted
    )
  
, swag_job_profiles as (
    select 
      u.uuid as user_uuid, u.id, lower(u.email) as email, u.created_at as user_created_at
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
      and ui.user_verified_at is not null
      and (len(ui.country_code) is null or len(ui.country_code)!=3)    
  )
    
select e.id
  , p.user_uuid
  , p.id as user_id
  , p.email
  , e.trim_job_title as job_title
  , e.job_title_without_seniority
  , {{ job_title_seniority_group('job_title_seniority') }} as job_title_seniority
  , e.industry_standard_job_title
  , e.company
  , e.summary
  , e.current_job
  , e.start_date
  , e.end_date
  , e.gap_years_to_date
  , e.duration_years
from 
  swag_job_profiles p 
  join candidate_experience e on p.id = e.user_id
