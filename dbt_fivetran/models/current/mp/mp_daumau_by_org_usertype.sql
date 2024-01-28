{{
    config(
        materialized='incremental',
        alias='daumau_by_org_usertype'
    )
}}

with dates as (
select
          DATEADD('day', -generated_number::int, (current_date + 1)) date
        from ({{ dbt_utils.generate_series(upper_bound=600) }})
          where "date" < (select date_trunc('day', max("timestamp")) from {{ ref('customers_events') }})
{% if is_incremental() %}          
          and date > (select max(date) from  {{ this }})     
{% endif %}             
)

, org_usertype_dau as (
        select
          DATE_TRUNC('day', e.timestamp) as "date"          
          , e.organisation_id
  		  , e.user_type
          , count(distinct coalesce(e.user_id, e.user_email)) as daily_users
        from
          {{ ref('customers_events') }} as e       
        where "timestamp" < (select date_trunc('day', max("timestamp")) from {{ ref('customers_events') }})   
        group by
          1, 2, 3
      )
, org_usertype_mau as (
        select
          dates.date          
        , e.organisation_id
  		, e.user_type
        , count(distinct coalesce(e.user_id, e.user_email)) as monthly_users
        from
          dates
          join {{ ref('customers_events') }} as e on
          e.timestamp < dateadd(day, 1, dates.date)
          and e.timestamp > dateadd(day, -29, dates.date)
        group by
          1, 2, 3
      )

select
      m.date      
	, m.organisation_id
	, m.user_type
    , coalesce(daily_users, 0) as daily_users
      , monthly_users
      , coalesce(daily_users, 0) / monthly_users :: float as dau_mau
    from
      org_usertype_mau m
      left join org_usertype_dau d on
        m.date = d.date 
                  and m.organisation_id = d.organisation_id
        and m.user_type = d.user_type