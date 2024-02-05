




select
    
    
    
    count(distinct user_actively_employed) as filler_column

from "dev"."exports"."exports_braze_users"

  

having count(distinct user_actively_employed) = 1


