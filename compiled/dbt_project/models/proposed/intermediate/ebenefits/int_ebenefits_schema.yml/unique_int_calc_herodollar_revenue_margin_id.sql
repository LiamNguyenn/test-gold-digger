
    
    

select
    id as unique_field,
    count(*) as n_records

from "dev"."intermediate"."int_calc_herodollar_revenue_margin"
where id is not null
group by id
having count(*) > 1


