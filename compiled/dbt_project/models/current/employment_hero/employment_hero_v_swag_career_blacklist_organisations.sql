

-- objects and group condition
select o.id as organisation_id    
from "dev"."postgres_public"."organisations" as o
left join(
        select distinct target_id as uuid
    from "dev"."feature_flag_public"."features" as f
    join "dev"."feature_flag_public"."features_target_objects" as fto on f.id = fto.feature_id
    join "dev"."feature_flag_public"."target_objects" as tob on fto.target_object_id = tob.id
    where code = 'swag_ats_internal_careers_release'
        and not f._fivetran_deleted
        and not fto._fivetran_deleted
        and not tob._fivetran_deleted        
  ) as bl on o.uuid = bl.uuid
left join (
    select distinct o.id as organisation_id
from "dev"."feature_flag_public"."features" f
join "dev"."feature_flag_public"."features_target_groups" ftg on ftg.feature_id = f.id 
join "dev"."feature_flag_public"."target_groups" tg on ftg.target_group_id = tg.id 
join "dev"."postgres_public"."organisations" o on tg.target_type = 'organisation' and tg.conditions like '%:' || o.id || '.%'
where f.code = 'swag_ats_internal_careers_release'
    and not f._fivetran_deleted
    and not ftg._fivetran_deleted
    and not tg._fivetran_deleted
    and not o._fivetran_deleted    
) as g on g.organisation_id = o.id 
where not o._fivetran_deleted 
    and (bl.uuid is not null or g.organisation_id is not null)