
    
    

select
    (month || residential_state) as unique_field,
    count(*) as n_records

from "dev"."employment_index"."median_hours_worked_state"
where (month || residential_state) is not null
group by (month || residential_state)
having count(*) > 1


