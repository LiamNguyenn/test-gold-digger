
    
    

select
    (month || employment_type) as unique_field,
    count(*) as n_records

from "dev"."employment_index"."median_hours_worked_employment_type"
where (month || employment_type) is not null
group by (month || employment_type)
having count(*) > 1


