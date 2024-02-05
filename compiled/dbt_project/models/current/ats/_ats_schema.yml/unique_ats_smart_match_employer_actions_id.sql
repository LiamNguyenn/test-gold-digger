
    
    

select
    id as unique_field,
    count(*) as n_records

from "dev"."ats"."smart_match_employer_actions"
where id is not null
group by id
having count(*) > 1


