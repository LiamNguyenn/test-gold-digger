
    
    

select
    organisation_id as unique_field,
    count(*) as n_records

from "dev"."employment_hero"."_v_ei_organisations"
where organisation_id is not null
group by organisation_id
having count(*) > 1


