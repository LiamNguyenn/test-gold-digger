

-- The blacklisted orgs are currently stored in ONE target group condition field: tg.conditions 
with blacklisted_org_users as (
    select user_id          
    -- any employing orgs not on the blacklist: not blacklisted        
    , case when sum(1) = 0 then false -- not employed
        when sum(case when bo.organisation_id is null then 1 else 0 end) > 0 then false --employed by at least 1 org not on blacklist
        else true end as is_blacklisted 
    from "dev"."postgres_public"."members" m 
    join "dev"."postgres_public"."users" u on m.user_id = u.id 
    left join "dev"."ebenefits"."_v_money_pillar_blacklist_organisations" bo on bo.organisation_id = m.organisation_id
    where not m._fivetran_deleted
        and not u._fivetran_deleted
        and m.active
    group  by 1
)

select coalesce(u.id, bou.user_id) as user_id
-- user in blacklist or employed by blacklisted orgs
from (
        select distinct u.email as user_email
        from "dev"."feature_flag_public"."features" f
        join "dev"."feature_flag_public"."features_target_groups" ftg on ftg.feature_id = f.id 
        join "dev"."feature_flag_public"."target_groups" tg on ftg.target_group_id = tg.id 
        join "dev"."postgres_public"."users" u on tg.target_type = 'member' and tg.conditions like '%"' || u.email || '"%'
        where f.code = 'eben_money_pillar_black_list'
            and not f._fivetran_deleted
            and not ftg._fivetran_deleted
            and not tg._fivetran_deleted
            and not u._fivetran_deleted        
    )d 
    join "dev"."postgres_public"."users" u on d.user_email = u.email 
    full outer join blacklisted_org_users bou on bou.user_id = u.id
    where bou.is_blacklisted is null or bou.is_blacklisted