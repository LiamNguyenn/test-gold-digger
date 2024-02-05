
    
    

select
    member_id as unique_field,
    count(*) as n_records

from "dev"."salary_guide"."employment_hero_v_au_fulltime_job_title_salary"
where member_id is not null
group by member_id
having count(*) > 1


