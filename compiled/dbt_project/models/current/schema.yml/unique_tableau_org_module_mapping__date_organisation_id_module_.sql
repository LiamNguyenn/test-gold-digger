
    
    

select
    (date || organisation_id || module) as unique_field,
    count(*) as n_records

from "dev"."tableau"."tableau_org_module_mapping"
where (date || organisation_id || module) is not null
group by (date || organisation_id || module)
having count(*) > 1


