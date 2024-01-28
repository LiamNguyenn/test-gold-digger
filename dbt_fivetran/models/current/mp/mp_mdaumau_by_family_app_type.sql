{{
    config(
        materialized='incremental',
        alias='mdaumau_by_family_app_type'
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
  , module_events as (
    select
      e.user_id
        , e.timestamp
        , e.module
        , mo.product_family
        , case when app_version_string is not null then 'mobile' else 'web' end as app_type
    from
      {{ ref('customers_events') }} as e
      join {{ ref('customers_users') }} us on 
          e.user_id = us.user_uuid
      join {{ ref('customers_accounts') }} a on 
          us.account_list = a.external_id
          and a.account_stage != 'Churned'
      join {{ source('eh_product', 'module_ownership') }} mo on
          mo.event_module = e.module
          and mo.product_family is not null    
    where "timestamp" < (select date_trunc('day', max("timestamp")) from {{ ref('customers_events') }})
      and e.module != 'mobile'
      and e.module != 'others'
      and e.module != 'Sign In'
  )
  , product_family_app_type_dau as (
    select
      DATE_TRUNC('day', e.timestamp) as "date"
      , e.app_type
      , e.product_family
      , count(distinct e.user_id) as daily_users
    from
      module_events as e
    group by 1,2,3
  )
  , product_family_app_type_mau as (
    select
      dates.date
      , e.app_type
      , e.product_family
      , count(distinct e.user_id) as monthly_users
    from
      dates
      join module_events as e on
        e.timestamp < dateadd(day, 1, dates.date)
        and e.timestamp > dateadd(day, -29, dates.date)
    group by 1,2,3
  )

select
  m.date
  , m.app_type
  , m.product_family
  , coalesce(daily_users, 0) as daily_users
  , monthly_users
  , coalesce(daily_users, 0) / monthly_users :: float as dau_mau
from
  product_family_app_type_mau m
  left join product_family_app_type_dau d on
    m.date = d.date
    and m.product_family = d.product_family
    and m.app_type = d.app_type