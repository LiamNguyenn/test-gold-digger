
    
    



select (date || service_offering || country)
from "dev"."customer_support"."customer_support_project_status_daily"
where (date || service_offering || country) is null


