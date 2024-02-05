
    
    

select
    time_zone as unique_field,
    count(*) as n_records

from "dev"."zendesk"."stg_zendesk__time_zone"
where time_zone is not null
group by time_zone
having count(*) > 1


