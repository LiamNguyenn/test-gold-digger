
    
    

select
    date as unique_field,
    count(*) as n_records

from "dev"."mp"."swag_daumau"
where date is not null
group by date
having count(*) > 1


