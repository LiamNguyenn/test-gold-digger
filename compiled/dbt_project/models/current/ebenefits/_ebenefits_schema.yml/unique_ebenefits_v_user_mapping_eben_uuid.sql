
    
    

select
    eben_uuid as unique_field,
    count(*) as n_records

from "dev"."ebenefits"."_v_user_mapping"
where eben_uuid is not null
group by eben_uuid
having count(*) > 1


