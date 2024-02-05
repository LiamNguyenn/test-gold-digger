

select
  og.id as organisation_guides_id
  ,og.organisation_id
  ,case 
    when state = 0 then 'not started' 
    when state = 2 then 'completed' 
  end as state
  ,convert_timezone('Australia/Sydney', completed_at) as guide_completed_at
  ,og.reward_extra_trial_days
  ,og.guide_id
  ,g.type as guide_type
  ,g.name as guide_name
  ,g.requirements as guide_requirements
from
  "dev"."postgres_public"."organisations" o
  join "dev"."postgres_public"."organisation_guides" og on
    o.id = og.organisation_id
  join "dev"."postgres_public"."guides" g on
    og.guide_id = g.id
where
  not og._fivetran_deleted
  and not g._fivetran_deleted
  and not o._fivetran_deleted
  and not o.is_shadow_data
  -- the below condition is because org id = 47711 has guides before organisation created date; one off
  and (convert_timezone('Australia/Sydney', o.created_at) < guide_completed_at or guide_completed_at is null)