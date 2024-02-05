

-- The blacklisted orgs are currently stored in ONE target group condition field: tg.conditions 
select distinct o.id as organisation_id
from "dev"."feature_flag_public"."features" f
    join "dev"."feature_flag_public"."features_target_groups" ftg on ftg.feature_id = f.id 
    join "dev"."feature_flag_public"."target_groups" tg on ftg.target_group_id = tg.id 
    join "dev"."postgres_public"."organisations" o on tg.target_type = 'organisation' and tg.conditions like '%:' || o.id || '.%' 
    where f.code = 'eben_benefits_pillar_black_list'
    and not f._fivetran_deleted
    and not ftg._fivetran_deleted
    and not tg._fivetran_deleted
    and not o._fivetran_deleted