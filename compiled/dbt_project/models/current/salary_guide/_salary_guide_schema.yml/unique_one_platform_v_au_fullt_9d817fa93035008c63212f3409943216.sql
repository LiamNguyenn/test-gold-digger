
    
    

select
    (op_org_id || op_member_id) as unique_field,
    count(*) as n_records

from "dev"."salary_guide"."one_platform_v_au_fulltime_job_title_salary"
where (op_org_id || op_member_id) is not null
group by (op_org_id || op_member_id)
having count(*) > 1


