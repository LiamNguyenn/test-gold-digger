
    
    

select
    (month || age_group) as unique_field,
    count(*) as n_records

from "dev"."employment_index"."median_hours_worked_age_group"
where (month || age_group) is not null
group by (month || age_group)
having count(*) > 1


