{{ config(materialized='table') }}

with overlap as (
    select eh_member_id, kp_employee_id 
    from {{ref('one_platform_employees')}}
    where eh_member_id is not null 
        and kp_employee_id is not null
)

, all_jobs as (
    select 
    kps.job_title
    , processed_title
    , CONCAT('K', organisation_id) as op_org_id 
    , industry
    , residential_state
    , CONCAT('K', member_id) as op_member_id
    , annual_salary
    from {{ref('keypay_v_au_fulltime_job_title_default_salary')}} as kps 
    where member_id not in (select kp_employee_id from overlap)
union all
    select 
    ehs.job_title
    , processed_title
    , CONCAT('E', organisation_id) as op_org_id  
    , industry
    , residential_state
    , CONCAT('E', member_id) as op_member_id  
    , annual_salary
    from {{ref('employment_hero_v_au_fulltime_job_title_salary')}} as ehs
)

select 
    -- Extract the seniority from the job title
    job_title
    , {{ job_title_without_seniority('processed_title') }} AS processed_title
    , {{ job_title_seniority('processed_title')}} AS seniority
    , op_org_id
    , industry
    , residential_state
    , op_member_id  
    , annual_salary
from all_jobs