
    
    

select
    (month || industry) as unique_field,
    count(*) as n_records

from "dev"."employment_index"."median_hours_worked_industry"
where (month || industry) is not null
group by (month || industry)
having count(*) > 1


