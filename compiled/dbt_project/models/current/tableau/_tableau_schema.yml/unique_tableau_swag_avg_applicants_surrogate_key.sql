
    
    

select
    surrogate_key as unique_field,
    count(*) as n_records

from "dev"."tableau"."swag_avg_applicants"
where surrogate_key is not null
group by surrogate_key
having count(*) > 1


