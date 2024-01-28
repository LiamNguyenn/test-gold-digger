{{ config(alias='smart_match_employees') }}

	select e.id as member_id
	, e.user_id
	, u.uuid as user_uuid
	, e.organisation_id
	, e.work_country
	, e.trim_job_title as latest_job_title
	, {{ job_title_without_seniority('trim_job_title') }} AS job_title_without_seniority
	, {{ job_title_seniority_group('job_title_seniority')}} as job_title_seniority
	, e.industry_standard_job_title
	, e.latest_employment_type
	, datediff('year', h.start_date, current_date) as job_title_tenure
	, datediff('year', ha.first_start_date, current_date) as org_tenure
	, datediff('year', date_of_birth, current_date) as age
	, addr.city 
	, addr.postcode
	from (select *, trim(latest_job_title) as trim_job_title, lower({{ job_title_seniority('trim_job_title')}}) AS job_title_seniority from  {{ ref('employment_hero_employees') }}) e
	join postgres_public.users u on e.user_id = u.id
	join {{ ref('employment_hero_organisations') }} o on e.organisation_id = o.id
	left join {{ current_row('postgres_public', 'employment_histories', 'member_id') }} h
		on e.id = h.member_id
	left join (select member_id, min(start_date) as first_start_date from {{ source('postgres_public', 'employment_histories') }} group by 1) ha on ha.member_id = e.id
	left join {{ source('postgres_public', 'addresses') }} addr on addr.id = e.address_id and not addr._fivetran_deleted
	where e.active
		and o.is_paying_eh
