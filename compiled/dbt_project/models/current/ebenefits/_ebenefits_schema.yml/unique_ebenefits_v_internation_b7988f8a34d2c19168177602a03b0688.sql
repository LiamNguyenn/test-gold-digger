
    
    

select
    organisation_id as unique_field,
    count(*) as n_records

from "dev"."ebenefits"."_v_international_benefits_on_off_organisations"
where organisation_id is not null
group by organisation_id
having count(*) > 1


