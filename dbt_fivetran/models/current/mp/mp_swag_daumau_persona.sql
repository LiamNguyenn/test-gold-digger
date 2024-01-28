{{
    config(
        materialized='incremental',
        alias='swag_daumau_persona'
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
  , swag_dau as (
    select
      DATE_TRUNC('day', e.timestamp) as "date"
      , persona
      , count(distinct e.user_email) as daily_users
    from
      {{ ref('customers_events') }} as e
    where
        e.app_version_string is not null
        and e.timestamp < (select date_trunc('day', max("timestamp")) from {{ ref('customers_events') }})
        {% if is_incremental() %}
          and e.timestamp > dateadd(day, 1, (select max("date") from {{ this }})) 
        {% endif %}      
    group by 1,2
  )
  , swag_mau as (
    select
      dates.date
      , persona
      , count(distinct e.user_email) as monthly_users
    from
      dates
      join {{ ref('customers_events') }} as e on
        e.timestamp < dateadd(day, 1, dates.date)
        and e.timestamp > dateadd(day, -29, dates.date)
        and e.app_version_string is not null
    where
        e.timestamp < (select date_trunc('day', max("timestamp")) from {{ ref('customers_events') }})
    group by 1,2
  )

select
  m.date
  , m.persona
  , coalesce(daily_users, 0) as daily_users
  , m.monthly_users
  , coalesce(daily_users, 0) / monthly_users :: float as dau_mau
from
  swag_mau m
  left join swag_dau d on
    m.date = d.date
    and m.persona = d.persona