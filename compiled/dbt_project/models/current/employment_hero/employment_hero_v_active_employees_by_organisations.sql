

select organisation_id
, count(*) as active_employees
from "dev"."postgres_public"."members" m
join "dev"."postgres_public"."users" u on m.user_id = u.id 
where  
    u.email !~* '.*(employmenthero|employmentinnovations|keypay|webscale|thinkei|p2m|power2motivate|test|demo|exacc|sandbox|\\+).*'

    and not m.system_manager 
    and not m.system_user 
    and not m.independent_contractor    
    and not m.is_shadow_data 
    and not u.is_shadow_data 
    and not m._fivetran_deleted
    and not u._fivetran_deleted
    and m.active
group  by 1