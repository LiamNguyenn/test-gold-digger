


with keypay_login as (
    select
        date_day,
        platform,
        eh_user_uuid, -- user uuid should naturally be null for all Keypay users
        NULL    as eh_user_id, -- hard coded as we specifically look for Keypay users in the upstream
        user_id as keypay_user_id,
        is_active,
        is_active_last_7_days,
        is_active_last_30_days

    from "dev"."intermediate"."int_spined_daily_user_signin_mp"

    where platform = 'keypay'

    union distinct

    select
        date_day,
        platform,
        NULL    as eh_user_uuid,
        NULL    as eh_user_id,
        user_id as keypay_user_id,
        is_active,
        is_active_last_7_days,
        is_active_last_30_days

    from "dev"."intermediate"."int_spined_daily_user_signin_keypay"
),

aggregated_keypay as (
    select
        date_day,
        platform,
        eh_user_uuid,
        eh_user_id,
        keypay_user_id,
        bool_or(is_active)              as is_active, -- to obtain the overall active status of Keypay users, EITHER through Keypay portal or through Swag
        bool_or(is_active_last_7_days)  as is_active_last_7_days, -- to obtain the overall active status of Keypay users over the last 7 days, EITHER through Keypay portal or through Swag
        bool_or(is_active_last_30_days) as is_active_last_30_days -- to obtain the overall active status of Keypay users over the last 30 days, EITHER through Keypay portal or through Swag

    from keypay_login

    group by 1,2,3,4,5
),

overall_union as (
    select
        date_day,
        platform,
        eh_user_uuid,
        user_id as eh_user_id,
        NULL    as keypay_user_id, -- forced to be null to enable surrogate key generation
        is_active,
        is_active_last_7_days,
        is_active_last_30_days

    from "dev"."intermediate"."int_spined_daily_user_signin_mp"

    where
        platform = 'employment_hero'
        and eh_user_uuid in (select distinct uuid from "dev"."staging"."stg_postgres_public__users") -- we need to exclude shadow data and users that have been deleted

    group by 1,2,3,4,5,6,7,8

    union distinct

    select *

    from aggregated_keypay
)

select * from overall_union