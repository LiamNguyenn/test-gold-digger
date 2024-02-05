
    
    

select
    organisation_id as unique_field,
    count(*) as n_records

from "dev"."ebenefits"."_v_benefits_pillar_blacklist_organisations"
where organisation_id is not null
group by organisation_id
having count(*) > 1


