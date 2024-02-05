
    
    

select
    (date || service_offering || country) as unique_field,
    count(*) as n_records

from "dev"."customer_support"."customer_support_project_status_daily"
where (date || service_offering || country) is not null
group by (date || service_offering || country)
having count(*) > 1


