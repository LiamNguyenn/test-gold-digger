

-- SELECT * FROM mp.swag_events 
-- SELECT user_email FROM customers.events WHERE persona = 'WZ User' and
-- app_version_string is not null
-- SELECT * FROM customers.accounts
-- logged in users
-- SELECT * FROM customers.events WHERE name ~* '.*(login).*' 
-- logged in not doing activities
-- and user_email not in (select user_email FROM customers.events WHERE name !~*
-- '.*(login).*')
with
    login_events as (
        select
            user_email,
            case when name ~* '.*(login).*' then 'Login' else 'Others' end as event_name,
            date_trunc('month', timestamp) as month,
            count(*) as login_count
        from "dev"."customers"."events"
        where event_name = 'Login' and persona = 'WZ User' and app_version_string is not null
        group by 1, 2, 3
        order by month, user_email
    ),
    other_events as (
        select
            user_email,
            case when name ~* '.*(login).*' then 'Login' else 'Others' end as event_name,
            date_trunc('month', timestamp) as month,
            count(*)
        from "dev"."customers"."events"
        where event_name = 'Others' and persona = 'WZ User' and app_version_string is not null
        group by 1, 2, 3
        order by month, user_email
    )

select le.user_email, le.event_name, le.month, login_count
from login_events le
left join other_events oe on le.user_email = oe.user_email and le.month = oe.month
where oe.event_name is null