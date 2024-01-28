{{
    config(
        materialized='incremental',
        alias='swag_daumau_country'
    )
}}

with 
  dates as (
    select
      DATEADD('day', -generated_number::int, (current_date + 1)) date
    from ({{ dbt_utils.generate_series(upper_bound=365) }})
    where
        "date" < (select date_trunc('day', max("timestamp")) from {{ ref('customers_events') }})
        {% if is_incremental() %}
            and "date" > (SELECT MAX("date") FROM {{ this }} ) 
        {% endif %}   
  )
  , user_country as (
    select
        mu.eh_platform_user_id
        ,u.uuid
        ,coalesce(mu.country, mu.eh_platform_employment_location) as country
    from 
        {{ref('marketing_users_from_snapshot')}} mu
        left join {{ source('postgres_public', 'users') }} u on
            mu.eh_platform_user_id = u.id
  )
  , swag_events as ( 
    select e.user_id, e.user_email, e.timestamp, uc.country
    from 
      {{ ref('customers_events') }} e
      left join user_country uc on
        e.user_id = uc.uuid
    where 
      e.app_version_string is not null
      and e.timestamp < (select date_trunc('day', max("timestamp")) from {{ ref('customers_events') }})
    
  )
  , swag_country_dau as (
    select
      DATE_TRUNC('day', e.timestamp) as "date"
      , e.country
      , count(distinct e.user_email) as daily_users
    from
      swag_events as e
    group by 1,2
  )
  , swag_country_mau as (
    select
      dates.date
      , e.country
      , count(distinct e.user_email) as monthly_users
    from
      dates
      join swag_events as e on
        e.timestamp < dateadd(day, 1, dates.date)
        and e.timestamp > dateadd(day, -29, dates.date)
    group by 1,2
  )

select
  m.date
  , m.country
  , coalesce(daily_users, 0) as daily_users
  , monthly_users
  , coalesce(daily_users, 0) / monthly_users :: float as dau_mau
from
  swag_country_mau m
  left join swag_country_dau d on
    m.date = d.date
    and m.country = d.country