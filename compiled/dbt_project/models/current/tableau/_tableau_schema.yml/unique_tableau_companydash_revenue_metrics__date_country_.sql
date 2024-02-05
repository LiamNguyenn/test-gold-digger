
    
    

select
    (date || country) as unique_field,
    count(*) as n_records

from "dev"."tableau"."tableau_companydash_revenue_metrics"
where (date || country) is not null
group by (date || country)
having count(*) > 1


