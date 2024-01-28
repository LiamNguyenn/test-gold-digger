{{ config(alias='smart_match_employers') }}
-- to be added: Employer side member id

with shortlisted as (
 Select o.uuid as org_uuid
  , o.id as org_id
  , cj.user_id as candidate_user_uuid
  , cj.job_id
  , j.job_title
  , {{ job_title_without_seniority('job_title') }} AS job_title_without_seniority
  , {{ job_title_seniority_group('job_title_seniority') }} as job_title_seniority
  , j.job_sector
  , j.industry
  , j.country
  , j.employment_type
  , j.job_description
  , j.candidate_location
  , j.is_remote_job
  , j.workplace_type
  , cj.created_at
  , cj._fivetran_deleted
  , cj.id
From {{source('ats_public', 'candidate_jobs')}} cj
join (select *, lower({{ job_title_seniority('job_title')}}) AS job_title_seniority from {{ ref('ats_jobs_created') }}) as j on cj.job_id = j.job_id
join {{ref('employment_hero_organisations')}} o on j.organisation_id = o.id
Where cj.source_name = 'Shortlisted from Saved candidate'
and not j.is_test_job
and is_paying_eh
)

, saved as (
	Select distinct o.uuid as org_uuid
  , o.id as org_id
  , u.uuid as candidate_user_uuid
  , sc.job_saved_for
  , sc.created_at
  , o.industry
  , o.country
  , sc.id
  , sc._fivetran_deleted
  , FIRST_VALUE(eh.title) over (partition by sc._fivetran_deleted, eh.member_id order by eh.start_date asc rows between unbounded preceding and unbounded following) as employer_job_title
  from {{source('postgres_public', 'saved_candidates')}} sc
  join {{ref('employment_hero_organisations')}} o on o.id = sc.organisation_id
  join {{source('postgres_public', 'users')}} u on u.id = sc.user_id
  left join {{ ref('employment_hero_employees') }}  he on he.id = sc.author_id
  left join {{ source('postgres_public', 'employment_histories') }} eh on eh.member_id = he.id and sc.created_at > eh.start_date and not eh._fivetran_deleted
  where o.is_paying_eh
)

select coalesce(l.org_uuid, s.org_uuid) as org_uuid
  , coalesce(l.org_id, s.org_id) as org_id
  , coalesce(l.candidate_user_uuid, s.candidate_user_uuid) as candidate_user_uuid
  , coalesce(l.country, s.country) as country
  , s.employer_job_title
  , s.job_saved_for
  , s.created_at as saved_at
  , l.job_title as shortlisted_job_title
  , l.created_at as shortlisted_at
  , l.job_title_without_seniority
  , l.job_title_seniority
  , l.job_id
  , l.job_sector
  , l.industry
  , l.employment_type
  , l.job_description
  , l.candidate_location
  , l.is_remote_job
  , l.workplace_type
from ( 
	select * from shortlisted
    where id in (
        select
            FIRST_VALUE(id) over(partition by org_uuid, candidate_user_uuid order by _fivetran_deleted asc, created_at desc rows between unbounded preceding and unbounded following)
        from
            shortlisted
        )
    ) as l 
full outer join 
	(
	select * from saved 
	where id in (
        select
            FIRST_VALUE(id) over(partition by org_uuid, candidate_user_uuid order by _fivetran_deleted asc, created_at desc rows between unbounded preceding and unbounded following)
        from saved
    )        
) as s on l.org_uuid = s.org_uuid and l.candidate_user_uuid = s.candidate_user_uuid 
