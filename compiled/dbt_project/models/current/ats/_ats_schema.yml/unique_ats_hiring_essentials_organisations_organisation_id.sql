
    
    

select
    organisation_id as unique_field,
    count(*) as n_records

from "dev"."ats"."hiring_essentials_organisations"
where organisation_id is not null
group by organisation_id
having count(*) > 1


