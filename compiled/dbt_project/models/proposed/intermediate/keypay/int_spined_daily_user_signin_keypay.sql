

with user_signin as (
    select
        date::date        as signin_date,
        logged_in_user_id as user_id,
        'keypay'::varchar as platform

    from "dev"."keypay"."system_event" as events

    where
        events.logged_in_user_id is not NULL
    

    group by 1,2,3
),

all_dates as (
    select date_day

    from "dev"."staging"."stg_dates__date_spine"
),

first_active_days as (
    select
        platform,
        user_id,
        min(signin_date)::date as first_active_day

    from user_signin

    group by 1,2
),

spined as (
    select
        first_active_days.platform,
        first_active_days.user_id,
        all_dates.date_day::date

    from first_active_days

    left join all_dates
        on first_active_days.first_active_day <= all_dates.date_day

    where
        all_dates.date_day < current_date
    --noqa: LT02
        and all_dates.date_day > dateadd('day', -30, (select max(date_day) from "dev"."intermediate"."int_spined_daily_user_signin_keypay")) -- we need to include the last 30 days in each incremental run to ensure the has_active_subscription window function continue to work
),

filled as (
    select
        spined.date_day,
        spined.platform,
        spined.user_id,
        user_signin.signin_date is not NULL                                                                                                                  as is_active,
        max(case when is_active then 1 else 0 end) over (partition by spined.user_id order by spined.date_day rows between 6 preceding and current row) > 0  as is_active_last_7_days,
        max(case when is_active then 1 else 0 end) over (partition by spined.user_id order by spined.date_day rows between 29 preceding and current row) > 0 as is_active_last_30_days

    from spined

    left join user_signin
        on
            spined.user_id = user_signin.user_id
            and spined.date_day = user_signin.signin_date
)

select * from filled