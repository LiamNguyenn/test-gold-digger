
    
    

select
    user_email as unique_field,
    count(*) as n_records

from "dev"."employment_hero"."_v_employees_first_time_swag_app"
where user_email is not null
group by user_email
having count(*) > 1


