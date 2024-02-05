
    
    

select
    (date || group_name || country) as unique_field,
    count(*) as n_records

from "dev"."customer_support"."customer_support_ticket_key_metrics"
where (date || group_name || country) is not null
group by (date || group_name || country)
having count(*) > 1


