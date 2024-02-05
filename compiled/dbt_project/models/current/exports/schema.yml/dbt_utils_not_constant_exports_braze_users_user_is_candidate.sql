




select
    
    
    
    count(distinct user_is_candidate) as filler_column

from "dev"."exports"."exports_braze_users"

  

having count(distinct user_is_candidate) = 1


