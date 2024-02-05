
    
    

select
    (date || account_id ) as unique_field,
    count(*) as n_records

from "dev"."mp"."daumau_by_account"
where (date || account_id ) is not null
group by (date || account_id )
having count(*) > 1


