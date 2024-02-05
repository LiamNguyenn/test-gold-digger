



select
    1
from (select * from "dev"."exports"."exports_braze_user_events" where event_name != 'candidate_profile_completed' and event_name != 'candidate_cv_uploaded' and event_name != 'candidate_public_profile') dbt_subquery

where not(event_prop_public_profile_enabled is null)

