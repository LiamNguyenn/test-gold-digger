





with validation_errors as (

    select
        external_id, updated_at
    from "dev"."exports"."exports_braze_user_profile_payloads"
    group by external_id, updated_at
    having count(*) > 1

)

select *
from validation_errors


