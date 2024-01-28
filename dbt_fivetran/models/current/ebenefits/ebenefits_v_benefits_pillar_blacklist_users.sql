{{ config(materialized='view', alias='_v_benefits_pillar_blacklist_users') }}

-- The blacklist is currently stored in ONE target group condition field: tg.conditions 
with blacklisted_org_users as (
    select user_id
    -- any employing orgs not on the blacklist: not blacklisted
    , case when sum(1) = 0 then false -- not employed
        when sum(case when not m._fivetran_deleted and m.active and (bo.organisation_id is null) then 1 else 0 end) > 0 then false --employed by at least 1 org not on blacklist
        else true end as is_blacklisted 
    from {{ source('postgres_public', 'members') }} m 
    join {{ source('postgres_public', 'users') }} u on m.user_id = u.id 
    left join {{ref('ebenefits_v_benefits_pillar_blacklist_organisations')}} bo on bo.organisation_id = m.organisation_id
    where not m._fivetran_deleted
        and not u._fivetran_deleted
        and m.active
    group  by 1
)

select coalesce(d.user_id, bou.user_id) as user_id    
from (
        select distinct u.id as user_id
        from {{ source('feature_flag_public', 'features') }} f
        join {{ source('feature_flag_public', 'features_target_groups') }} ftg on ftg.feature_id = f.id 
        join {{ source('feature_flag_public', 'target_groups') }} tg on ftg.target_group_id = tg.id 
        join {{ source('postgres_public', 'users') }} u on tg.target_type = 'member' and tg.conditions like '%"' || u.email || '"%'
        where f.code = 'eben_benefits_pillar_black_list'
            and not f._fivetran_deleted
            and not ftg._fivetran_deleted
            and not tg._fivetran_deleted
            and not u._fivetran_deleted        
    )d    
    full outer join blacklisted_org_users bou on bou.user_id = d.user_id
    where bou.is_blacklisted is null or bou.is_blacklisted 