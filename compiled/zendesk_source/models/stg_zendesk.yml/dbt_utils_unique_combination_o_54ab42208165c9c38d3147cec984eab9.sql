





with validation_errors as (

    select
        time_zone, year
    from "dev"."zendesk"."stg_zendesk__daylight_time"
    group by time_zone, year
    having count(*) > 1

)

select *
from validation_errors


