





with validation_errors as (

    select
        date_day, platform, eh_user_uuid, keypay_user_id
    from "dev"."intermediate"."int_union_daily_user_signin"
    group by date_day, platform, eh_user_uuid, keypay_user_id
    having count(*) > 1

)

select *
from validation_errors


