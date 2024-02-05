
    
    

select
    date as unique_field,
    count(*) as n_records

from "dev"."mp"."wau"
where date is not null
group by date
having count(*) > 1

