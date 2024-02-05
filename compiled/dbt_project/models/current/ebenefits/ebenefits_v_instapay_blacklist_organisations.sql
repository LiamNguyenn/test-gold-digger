

select distinct target_id as organisation_uuid
from "dev"."feature_flag_public"."features" as f
join "dev"."feature_flag_public"."features_target_objects" as fto on f.id = fto.feature_id
join "dev"."feature_flag_public"."target_objects" as tob on fto.target_object_id = tob.id
join "dev"."postgres_public"."organisations" o on target_id = o.uuid 
where code in ('e2p0_instapay_refused', 'instapay_refused_by_swag', 'instapay_refused_employment_innovations')
    and not f._fivetran_deleted
    and not fto._fivetran_deleted
    and not tob._fivetran_deleted
    and not o._fivetran_deleted