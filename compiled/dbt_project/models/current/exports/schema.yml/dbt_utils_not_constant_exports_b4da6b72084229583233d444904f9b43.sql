




select
    
    
    
    count(distinct candidate_recent_job_title) as filler_column

from "dev"."exports"."exports_braze_users"

  

having count(distinct candidate_recent_job_title) = 1


