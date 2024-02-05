

with eh_org_users as (
    select user_id            
        -- employed by at least one organisation that's not refused         
        , case when sum(case when bo.organisation_uuid is null then 1 else 0 end) > 0 then true else false end as is_active_member        
    from "dev"."postgres_public"."members" m
    join "dev"."postgres_public"."users" u on m.user_id = u.id 
    join "dev"."postgres_public"."organisations" o on m.organisation_id = o.id 
    left join "dev"."ebenefits"."_v_worklife_refused_organisations" bo on bo.organisation_uuid = m.organisation_id
    -- includes independent contractors 
        where not m._fivetran_deleted
        and not u._fivetran_deleted
        and not o._fivetran_deleted    
        and m.active
    group by 1
)

-- only checking user_infos, not member country  
, au_users as (
    select u.id as user_id                 
        , case when sum(case when upper(ui.country_code) in ('AU', 'AUS') then 1 else 0 end) > 0 then true else false end as is_au        
    from "dev"."postgres_public"."users" u    
    join "dev"."postgres_public"."user_infos" ui on u.id = ui.user_id
        where not ui._fivetran_deleted
        and not u._fivetran_deleted
    group by 1
)

select u.id as user_id    
    , case when au.is_au is null then null                                                          -- not selected country picker        
        when not au.is_au then false                                                                -- not selected AU 
        when eu.is_active_member is null or not eu.is_active_member then false                      -- not active employee of non-refused org       
        when au.is_au and eu.is_active_member then true                                             -- country picker: AU, employed by non-refused org
        end as store_enabled
from "dev"."postgres_public"."users" u
left join eh_org_users eu on u.id = eu.user_id
left join au_users au on u.id = au.user_id
where not u._fivetran_deleted