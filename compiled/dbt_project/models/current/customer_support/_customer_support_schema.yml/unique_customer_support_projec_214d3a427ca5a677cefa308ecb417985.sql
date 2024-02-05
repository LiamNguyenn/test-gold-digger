
    
    

select
    (date || data_type || country || sub_type || sub_value) as unique_field,
    count(*) as n_records

from "dev"."customer_support"."customer_support_project_key_metrics"
where (date || data_type || country || sub_type || sub_value) is not null
group by (date || data_type || country || sub_type || sub_value)
having count(*) > 1


