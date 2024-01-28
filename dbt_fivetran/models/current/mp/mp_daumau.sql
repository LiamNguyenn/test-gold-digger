{{
    config(
        materialized='incremental',
        alias='daumau'
    )
}}


with dates as (
select
          DATEADD('day', -generated_number::int, (current_date + 1)) as "date"
        from ({{ dbt_utils.generate_series(upper_bound=365) }})
          where "date" < (select date_trunc('day', max("timestamp")) from {{ ref('customers_events') }})
{% if is_incremental() %}
        and "date" > (SELECT MAX("date") FROM {{ this }} ) 
{% endif %}          
)                               
      , dau as (
        select
          DATE_TRUNC('day', e.timestamp) as "date"
          , count(distinct coalesce(e.user_id, e.user_email)) as daily_users
        from
          {{ ref('customers_events') }} as e          
        where          
        e.timestamp < (select date_trunc('day', max("timestamp")) from {{ ref('customers_events') }})
{% if is_incremental() %}
          and e.timestamp > dateadd(day, 1, (select max("date") from {{ this }})) 
{% endif %}             
        group by
          1
      )
      , mau as (
        select
          dates.date
          , count(distinct coalesce(e.user_id, e.user_email)) as monthly_users
        from
          dates join {{ ref('customers_events') }} as e on e.timestamp < dateadd(day, 1, dates.date)
          and e.timestamp > dateadd(day, -29, dates.date)          
        where          
          e.timestamp < (select date_trunc('day', max("timestamp")) from {{ ref('customers_events') }})
        group by
          1
      )

select
      m.date      
      , coalesce(daily_users, 0) as daily_users
      , monthly_users
      , coalesce(daily_users, 0) / monthly_users :: float as dau_mau
    from
      mau m
      left join dau d on
        m.date = d.date