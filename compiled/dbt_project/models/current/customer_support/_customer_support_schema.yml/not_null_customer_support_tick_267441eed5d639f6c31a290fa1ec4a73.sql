
    
    



select (date || group_name || country)
from "dev"."customer_support"."customer_support_ticket_key_metrics"
where (date || group_name || country) is null


