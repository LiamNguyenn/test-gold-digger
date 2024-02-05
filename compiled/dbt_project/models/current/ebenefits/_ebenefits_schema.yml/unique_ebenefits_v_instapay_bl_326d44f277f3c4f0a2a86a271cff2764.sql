
    
    

select
    organisation_uuid as unique_field,
    count(*) as n_records

from "dev"."ebenefits"."_v_instapay_blacklist_organisations"
where organisation_uuid is not null
group by organisation_uuid
having count(*) > 1


