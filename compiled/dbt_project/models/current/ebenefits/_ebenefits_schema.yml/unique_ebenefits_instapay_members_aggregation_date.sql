
    
    

select
    date as unique_field,
    count(*) as n_records

from "dev"."ebenefits"."instapay_members_aggregation"
where date is not null
group by date
having count(*) > 1


