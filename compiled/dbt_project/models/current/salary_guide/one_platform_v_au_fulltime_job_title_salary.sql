

with overlap as (
    select eh_member_id, kp_employee_id 
    from "dev"."one_platform"."employees"
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
    from "dev"."salary_guide"."keypay_v_au_fulltime_job_title_default_salary" as kps 
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
    from "dev"."salary_guide"."employment_hero_v_au_fulltime_job_title_salary" as ehs
)

select 
    -- Extract the seniority from the job title
    job_title
    ,  
case
    when regexp_replace(processed_title, '^(Deputy |Casual )', '', 1, 'i') ~* 'assistant accountant' 
        then INITCAP(trim(regexp_replace(regexp_replace(processed_title, '^(Deputy |Casual )', '', 1, 'i'), '^(Graduate |Trainee |Associate |Apprentice |Junior |Intermediate |Senior |Lead |Principal |Chief |Head |Executive |Vice |Managing )', '', 1, 'i'))) 
    when regexp_replace(processed_title, '^(Deputy |Casual )', '', 1, 'i') !~* '^(Graduate |Associate |Assistant |Apprentice |Junior |Intermediate |Senior |Lead |Principal |Chief |Head |Executive |Vice |Managing )(of |to |\or |\and )'
        and regexp_replace(processed_title, '^(Deputy |Casual )', '', 1, 'i') !~* '^(Chief |Executive |Lead ).*(officer|assistant|generator).*'
    then INITCAP(trim(regexp_replace(regexp_replace(processed_title, '^(Deputy |Casual )', '', 1, 'i'), '^(Graduate |Trainee |Associate |Assistant |Apprentice |Junior |Intermediate |Senior |Lead |Principal |Chief |Head |Executive |Vice |Managing )', '', 1, 'i'))) 
    else INITCAP(regexp_replace(processed_title, '^(Deputy |Casual )', '', 1, 'i')) end
 AS processed_title
    ,  
case when regexp_replace(processed_title, '^(Deputy |Casual )', '', 1, 'i') ~ '^(Apprentice |Graduate |Trainee |Junior |Intermediate |Senior |Managing |Lead |Leader |Head |Vice |Manager |Director |Chief )' 
        then trim(regexp_substr(regexp_replace(processed_title, '^(Deputy |Casual )', '', 1, 'i'), '^(Apprentice |Graduate |Trainee |Junior |Intermediate |Senior |Managing |Lead |Leader |Head |Vice |Manager |Director |Chief )', 1, 1, 'i'))        
    when regexp_replace(processed_title, '^(Deputy |Casual )', '', 1, 'i') !~* '^(Associate |Assistant |Principal |Executive )(of |to )'
        and trim(regexp_substr(regexp_replace(processed_title, '^(Deputy |Casual )', '', 1, 'i'), '^(Associate |Assistant |Principal |Executive )', 1, 1, 'i')) != ''
        then trim(regexp_substr(regexp_replace(processed_title, '^(Deputy |Casual )', '', 1, 'i'), '^(Associate |Assistant |Principal |Executive )', 1, 1, 'i'))
    when processed_title ~ '(^|\\W)Apprentice(\\W|$)' then 'Apprentice'
    when processed_title ~ '(^|\\W)Graduate(\\W|$)' then 'Graduate'
    when processed_title ~ '(^|\\W)Junior(\\W|$)' then 'Junior'
    when processed_title ~ '(^|\\W)Intermediate(\\W|$)' then 'Intermediate'
    when processed_title ~ '(^|\\W)Senior(\\W|$)' then 'Senior'    
    when processed_title ~ '(^|\\W)Managing(\\W|$)' then 'Managing'
    when processed_title ~ '(^|\\W)(Lead|Leader)(\\W|$)' then 'Lead'
    when processed_title ~ '(^|\\W)Trainee(\\W|$)' then 'Trainee'
    when processed_title ~ '(^|\\W)Head(\\W|$)' then 'Head'
    when processed_title ~ '(^|\\W)Vice(\\W|$)' then 'Vice'
    when processed_title ~ '(^|\\W)Manager(\\W|$)' then 'Manager'
    when processed_title ~ '(^|\\W)Director(\\W|$)' then 'Director'
    when processed_title ~ '(^|\\W)Chief(\\W|$)' then 'Chief'
    else null end
 AS seniority
    , op_org_id
    , industry
    , residential_state
    , op_member_id  
    , annual_salary
from all_jobs