{{ config(materialized='view', alias='_v_money_pillar_on_off_users') }}

with
-- only checking user_infos, not member country  
au_users as (
    select u.id as user_id         
        , case when sum(case when upper(ui.country_code) in ('AU', 'AUS') then 1 else 0 end) > 0 then true else false end as is_au
    from {{ source('postgres_public', 'users') }} u
    join {{ source('postgres_public', 'user_infos') }} ui on u.id = ui.user_id
    where not u._fivetran_deleted
        and not ui._fivetran_deleted
    group by 1
)

select u.id as user_id     
    , case when bl.user_id is not null then false    -- blacklisted 
        when au.is_au is null then null         -- not picked country
        when au.is_au and (bl.user_id is null) then true    -- country picker: AU, and not blacklisted
        end as money_enabled
from {{ source('postgres_public', 'users') }} u
left join {{ref('ebenefits_v_money_pillar_blacklist_users')}} bl on bl.user_id = u.id 
left join au_users au on u.id = au.user_id
where not u._fivetran_deleted
and (au.user_id is not null or bl.user_id is not null)