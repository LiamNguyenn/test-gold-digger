

select 
    lower(user_email) as user_email
    , min(timestamp) as first_time_swag_app 
from 
    "dev"."customers"."events"
where 
    app_version_string is not null
    and app_version_string ~* '^[2-9].*' -- build version must be greater than 2.0.0
    and timestamp>= '2023-02-20' -- swag app release
group by 1