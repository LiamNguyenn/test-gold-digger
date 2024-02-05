
    
    



select (date || group_name || country)
from "dev"."customer_support"."customer_support_ticket_status_daily"
where (date || group_name || country) is null


