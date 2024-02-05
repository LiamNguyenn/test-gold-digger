
    
    

select
    user_id as unique_field,
    count(*) as n_records

from "dev"."ebenefits"."_v_benefits_pillar_blacklist_users"
where user_id is not null
group by user_id
having count(*) > 1


