{{ config(materialized='view', alias='_v_active_employees_by_organisations') }}

select organisation_id
, count(*) as active_employees
from {{ source('postgres_public', 'members') }} m
join {{ source('postgres_public', 'users') }} u on m.user_id = u.id 
where  {{legit_emails('u.email')}}
    and not m.system_manager 
    and not m.system_user 
    and not m.independent_contractor    
    and not m.is_shadow_data 
    and not u.is_shadow_data 
    and not m._fivetran_deleted
    and not u._fivetran_deleted
    and m.active
group  by 1