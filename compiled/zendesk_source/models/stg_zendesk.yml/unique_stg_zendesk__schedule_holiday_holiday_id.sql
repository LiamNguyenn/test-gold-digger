
    
    

select
    holiday_id as unique_field,
    count(*) as n_records

from "dev"."zendesk"."stg_zendesk__schedule_holiday"
where holiday_id is not null
group by holiday_id
having count(*) > 1


