
    
    

select
    (month || company_size) as unique_field,
    count(*) as n_records

from "dev"."employment_index"."median_hours_worked_company_size"
where (month || company_size) is not null
group by (month || company_size)
having count(*) > 1


