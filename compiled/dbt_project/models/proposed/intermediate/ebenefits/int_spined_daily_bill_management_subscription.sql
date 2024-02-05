

with all_dates as (
    select date_day

    from "dev"."staging"."stg_dates__date_spine"
),

subscription as (
    select *

    from "dev"."intermediate"."int_union_bill_management_subscription"

    where subscription_status != 'submitted'
),

first_active_days as (
    select
        ebenefits_user_uuid,
        min(activated_at)::date as first_active_day

    from subscription

    group by 1
),

spined as (
    select
        first_active_days.ebenefits_user_uuid,
        all_dates.date_day::date

    from first_active_days

    left join all_dates
        on first_active_days.first_active_day <= all_dates.date_day

    where
        all_dates.date_day < current_date
     --noqa: LT02
            and all_dates.date_day > dateadd('day', -30, (select max(date_day) from "dev"."intermediate"."int_spined_daily_bill_management_subscription")) -- we need to include the last 30 days in each incremental run to ensure the has_active_subscription window function continue to work
    
),

filled as (
    select
        spined.date_day,
        spined.ebenefits_user_uuid,
        count(distinct subscription.subscription_id)                                                                                                         as active_subscription_count,
        max(active_subscription_count) over (partition by spined.ebenefits_user_uuid order by spined.date_day rows between 6 preceding and current row) > 0  as has_active_subscription_last_7_days,
        max(active_subscription_count) over (partition by spined.ebenefits_user_uuid order by spined.date_day rows between 29 preceding and current row) > 0 as has_active_subscription_last_30_days

    from spined

    left join subscription
        on
            spined.ebenefits_user_uuid = subscription.ebenefits_user_uuid
            and subscription.activated_at::date <= spined.date_day
            and (subscription.cancelled_at::date > spined.date_day or subscription.cancelled_at is NULL)

    group by 1, 2
),

user_mapping as (
    select
        ebenefits.*,
        eh_users.id as eh_user_id

    from "dev"."staging"."stg_ebenefits__user_created" as ebenefits

    left join "dev"."staging"."stg_postgres_public__users" as eh_users
        on ebenefits.eh_user_uuid = eh_users.uuid
),

mapped as (
    select
        filled.date_day,
        filled.ebenefits_user_uuid,
        user_mapping.eh_user_id,
        user_mapping.keypay_user_id,
        case when user_mapping.eh_user_id is not NULL then 'employment_hero' else 'keypay' end as platform,
        filled.active_subscription_count,
        filled.has_active_subscription_last_7_days,
        filled.has_active_subscription_last_30_days

    from filled

    inner join user_mapping
        on filled.ebenefits_user_uuid = user_mapping.ebenefits_user_uuid
)

select * from mapped