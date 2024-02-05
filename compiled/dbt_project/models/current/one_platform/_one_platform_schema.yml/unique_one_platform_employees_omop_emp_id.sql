
    
    

select
    omop_emp_id as unique_field,
    count(*) as n_records

from "dev"."one_platform"."employees"
where omop_emp_id is not null
group by omop_emp_id
having count(*) > 1


