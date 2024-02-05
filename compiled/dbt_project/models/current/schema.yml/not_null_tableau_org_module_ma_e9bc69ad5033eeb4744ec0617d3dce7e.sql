
    
    



select (date || organisation_id || module)
from "dev"."tableau"."tableau_org_module_mapping"
where (date || organisation_id || module) is null


