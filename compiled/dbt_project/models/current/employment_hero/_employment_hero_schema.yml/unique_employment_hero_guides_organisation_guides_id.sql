
    
    

select
    organisation_guides_id as unique_field,
    count(*) as n_records

from "dev"."employment_hero"."guides"
where organisation_guides_id is not null
group by organisation_guides_id
having count(*) > 1


