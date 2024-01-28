{{
    config(
        materialized='incremental',
        alias='daumau_by_platform'
    )
}}

with dates as (
select
          DATEADD('day', -generated_number::int, (current_date + 1)) date
        from ({{ dbt_utils.generate_series(upper_bound=180) }})
        where "date" < (select date_trunc('day', max("timestamp")) from {{ ref('customers_events') }})
{% if is_incremental() %}
        and date > (select max(date) from {{ this }} )
{% endif %}   
)
                               
, platform_events as (
        select
          user_id, user_email, "timestamp", platform
        from
          {{ ref('customers_events') }} 
        where app_version_string is not null and platform != ''         
        and "timestamp" < (select date_trunc('day', max("timestamp")) from {{ ref('customers_events') }})
      )
, platform_dau as (
        select
          DATE_TRUNC('day', timestamp) as "date"
          , platform          
          , count(distinct coalesce(user_id, user_email)) as daily_users
        from
          platform_events         
        group by
          1, 2
      )
      , platform_mau as (
        select
          dates.date
          , e.platform        
          , count(distinct coalesce(e.user_id, e.user_email)) as monthly_users
        from
          dates
          join platform_events as e on
          e.timestamp < dateadd(day, 1, dates.date)
          and e.timestamp > dateadd(day, -29, dates.date)
        group by
          1, 2
      )

select
      m.date
      , m.platform	
      , coalesce(daily_users, 0) as daily_users
      , monthly_users
      , coalesce(daily_users, 0) / monthly_users :: float as dau_mau
    from
      platform_mau m
      left join platform_dau d on
        m.date = d.date                   
        and m.platform = d.platform