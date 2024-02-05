
    
    

select
    (organisation_id || member_id) as unique_field,
    count(*) as n_records

from "dev"."salary_guide"."keypay_v_au_fulltime_job_title_default_salary"
where (organisation_id || member_id) is not null
group by (organisation_id || member_id)
having count(*) > 1


