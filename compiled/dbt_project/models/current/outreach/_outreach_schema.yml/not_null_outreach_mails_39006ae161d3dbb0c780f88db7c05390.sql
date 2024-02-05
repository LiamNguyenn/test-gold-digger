
    
    



select (id || state || prospect_name || step_name || sequence_name)
from "dev"."outreach"."outreach_mails"
where (id || state || prospect_name || step_name || sequence_name) is null


