
    
    

select
    member_id as unique_field,
    count(*) as n_records

from "dev"."ebenefits"."instapay_eligible_member_profile"
where member_id is not null
group by member_id
having count(*) > 1


