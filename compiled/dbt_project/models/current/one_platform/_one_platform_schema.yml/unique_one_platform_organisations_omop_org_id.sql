
    
    

select
    omop_org_id as unique_field,
    count(*) as n_records

from "dev"."one_platform"."organisations"
where omop_org_id is not null
group by omop_org_id
having count(*) > 1


