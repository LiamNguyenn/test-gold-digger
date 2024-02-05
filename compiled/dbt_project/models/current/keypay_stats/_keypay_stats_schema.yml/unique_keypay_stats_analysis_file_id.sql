
    
    

select
    id as unique_field,
    count(*) as n_records

from "dev"."keypay_stats"."analysis_file"
where id is not null
group by id
having count(*) > 1


