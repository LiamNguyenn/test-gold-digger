with
  internal_employees as (
    select
        _row
        , job_title
        , lower(seniority) as seniority
        , country
        , num_employees
        , similar_job_title
        , have_salary_range
        , better_match_job_title_with_salary_range
        , eng_band_ref
        , aon_job_code
        , standard_job_title
        , coalesce(p_10_salary, round(0.85*p_25_salary))::bigint as p10
        , p_25_salary::bigint as p25
        , p_50_salary::bigint as p50
        , p_75_salary::bigint as p75
        , coalesce(p_90_salary, round(1.05*p_75_salary))::bigint as p90
        , 'all'::varchar(256) as employment_type
        , 'all'::varchar(256) as residential_state
        , 'all'::varchar(256) as industry
        , null::bigint as samples
        , null::bigint as orgs
        , 'high'::varchar(256) as confidence_level
    from {{ source('salary_guide', 'eh_internal_salary_range') }}
    where eng_band_ref is null -- covered in engineering salary range, ignore countries that are not in engineering salary banding list
  )
--   case 1: have salary range (AU) but no good matching job titles with salary range (need to take AON data)
  , case1 as (
    select distinct
      standard_job_title as occupation
      , country, seniority, employment_type, residential_state, industry, samples, orgs
      , p10, p25, p50, p75, p90, confidence_level
    from internal_employees
    where 
      have_salary_range 
      and standard_job_title is not null
  )
  , case1_all_seniority as (
    select 
      occupation, country, 'all' as seniority, employment_type, residential_state, industry
      , samples, orgs, p10, p25, p50, p75, p90, confidence_level
    from case1
  )
--   case 2: dont have salary range (AU) and no good matching job titles with salary range (need to take AON data)
  , case2 as (
    select distinct
      similar_job_title as occupation
      , country, seniority, employment_type, residential_state, industry, samples, orgs
      , p10, p25, p50, p75, p90, confidence_level
    from internal_employees
    where 
      not have_salary_range
      and country = 'AU'
      and better_match_job_title_with_salary_range is null
  )
  , case2_all_seniority as (
    select 
      occupation, country, 'all' as seniority, employment_type, residential_state, industry
      , samples, orgs, p10, p25, p50, p75, p90, confidence_level
    from case2
  )
--   case 3: dont have salary range (outside AU) so definately need AON data
  , case3 as (
    select distinct
      coalesce(standard_job_title, better_match_job_title_with_salary_range, similar_job_title) as occupation
      , country, seniority, employment_type, residential_state, industry, samples, orgs
      , p10, p25, p50, p75, p90, confidence_level
    from 
      internal_employees 
    where
      country not in ('AU','TW','NL','KR','ID') -- no AON data for these countries (outside AU)
      and p50!=1 -- no AON data for these roles
  )
  , case3_all_seniority as (
    select 
      occupation, country, 'all' as seniority, employment_type, residential_state, industry
      , samples, orgs, p10, p25, p50, p75, p90, confidence_level
    from (
      select *, row_number() over (partition by occupation, country order by seniority) as rn
      from case3
      )
    where rn=1
  )

select 
    {{ dbt_utils.generate_surrogate_key(['occupation', 'country', 'seniority', 'employment_type', 'residential_state', 'industry']) }} as id
    , * 
from (
    select * from case1_all_seniority
    union 
    select * from case1
    union
    select * from case2_all_seniority
    union 
    select * from case2
    union
    select * from case3_all_seniority
    union 
    select * from case3
)