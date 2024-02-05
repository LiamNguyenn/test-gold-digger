
    
    

select
    month as unique_field,
    count(*) as n_records

from "dev"."employment_index"."median_hours_worked_aus"
where month is not null
group by month
having count(*) > 1


