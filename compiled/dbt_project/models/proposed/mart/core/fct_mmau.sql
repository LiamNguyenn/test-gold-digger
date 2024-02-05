select
    date_day,
    

  to_number(to_char(date_day::DATE,'YYYYMMDD'),'99999999')

 as dim_date_sk,
    md5(cast(coalesce(cast(platform as TEXT), '_dbt_utils_surrogate_key_null_') || '-' || coalesce(cast(eh_user_uuid as TEXT), '_dbt_utils_surrogate_key_null_') || '-' || coalesce(cast(keypay_user_id as TEXT), '_dbt_utils_surrogate_key_null_') as TEXT)) as dim_user_sk,
    platform,
    eh_user_uuid,
    keypay_user_id,
    is_active_last_30_days

from "dev"."intermediate"."int_union_daily_user_signin"