
    
    

select
    date_day as unique_field,
    count(*) as n_records

from "dev"."salesforce"."salesforce__daily_activity"
where date_day is not null
group by date_day
having count(*) > 1


