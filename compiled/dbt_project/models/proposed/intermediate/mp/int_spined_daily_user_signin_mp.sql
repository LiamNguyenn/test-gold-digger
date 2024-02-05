

with user_signin as (
    select
        events.event_timestamp::date as signin_date,
        eh_users.uuid                as eh_user_uuid,
        events.user_id,
        case
            when events.login_provider = 'eh' or events.eh_employee_id is not NULL then 'employment_hero'
            when events.login_provider = 'kp' then 'keypay'
        end                          as platform

    from "dev"."staging"."stg_mp__event" as events

    left join "dev"."staging"."stg_postgres_public__users" as eh_users
        on
            events.user_id = eh_users.id
            and events.eh_employee_id is not NULL

    where
        lower(events.name) in ('sign in', 'login success')
    and events.event_timestamp::date >= '2023-01-01' -- limit to 2023 onwards to keep it consistent as we only have Keypay data from 2023 onwards
),

all_dates as (
    select date_day

    from "dev"."staging"."stg_dates__date_spine"
),

first_active_days as (
    select
        platform,
        eh_user_uuid,
        user_id,
        min(signin_date)::date as first_active_day

    from user_signin

    group by 1,2,3
),

spined as (
    select
        first_active_days.platform,
        first_active_days.eh_user_uuid,
        first_active_days.user_id,
        all_dates.date_day::date

    from first_active_days

    left join all_dates
        on first_active_days.first_active_day <= all_dates.date_day

    where
        all_dates.date_day < current_date
     --noqa: LT02
            and all_dates.date_day > dateadd('day', -30, (select max(date_day) from "dev"."intermediate"."int_spined_daily_user_signin_mp")) -- we need to include the last 30 days in each incremental run to ensure the has_active_subscription window function continue to work
    
),

filled as (
    select
        spined.date_day,
        spined.platform,
        spined.eh_user_uuid,
        spined.user_id,
        user_signin.signin_date is not NULL                                                                                                                                   as is_active,
        max(case when is_active then 1 else 0 end) over (partition by spined.user_id, spined.platform order by spined.date_day rows between 6 preceding and current row) > 0  as is_active_last_7_days,
        max(case when is_active then 1 else 0 end) over (partition by spined.user_id, spined.platform order by spined.date_day rows between 29 preceding and current row) > 0 as is_active_last_30_days

    from spined

    left join user_signin
        on
            spined.user_id = user_signin.user_id
            and spined.platform = user_signin.platform
            and spined.date_day = user_signin.signin_date
)

select * from filled