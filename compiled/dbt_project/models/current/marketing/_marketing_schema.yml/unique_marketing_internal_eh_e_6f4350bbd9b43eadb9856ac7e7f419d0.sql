
    
    

select
    (work_country || month) as unique_field,
    count(*) as n_records

from "dev"."marketing"."internal_eh_employee_growth_country"
where (work_country || month) is not null
group by (work_country || month)
having count(*) > 1


