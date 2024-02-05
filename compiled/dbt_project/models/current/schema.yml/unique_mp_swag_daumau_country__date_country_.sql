
    
    

select
    (date || country ) as unique_field,
    count(*) as n_records

from "dev"."mp"."swag_daumau_country"
where (date || country ) is not null
group by (date || country )
having count(*) > 1


