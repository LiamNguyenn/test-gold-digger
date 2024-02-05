
    
    



select (date || data_type || country || sub_type || sub_value)
from "dev"."customer_support"."customer_support_project_key_metrics"
where (date || data_type || country || sub_type || sub_value) is null


