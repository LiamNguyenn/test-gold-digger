with t_matched as (
  select 
    t.job_title
    , t.processed_title
    , coalesce(m.matched_job_title, t.processed_title) as matched_title
    , seniority
    , op_org_id as organisation_id
    , industry
    , residential_state
    , op_member_id as member_id
    , annual_salary    
  from {{ ref('one_platform_v_au_fulltime_job_title_salary') }} t 
    join {{ ref('matched_job_titles') }} m on t.processed_title = m.processed_title
  ) 

select 
    job_title
    , processed_title
    , matched_title
    , {{ job_title_seniority_group('seniority')}} as seniority
    , organisation_id
    , industry
    , residential_state
    , member_id
    , annual_salary
    , case when stddev(annual_salary) over (partition by matched_title) !=0 then (annual_salary-avg(annual_salary) over (partition by matched_title)) 
        / (stddev(annual_salary) over (partition by matched_title)) else null end as z_score_title_salary
    , ntile(3) over (partition by matched_title, seniority, organisation_id order by annual_salary) as ntile_3_by_org
    , ntile(3) over (partition by matched_title, seniority, organisation_id, residential_state order by annual_salary) as ntile_3_by_org_state
from t_matched
