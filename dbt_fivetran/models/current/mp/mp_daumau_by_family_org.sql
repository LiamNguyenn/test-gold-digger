{{
    config(
        materialized='incremental',
        alias='daumau_by_family_org'
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
  , events as (
    select
      	e.user_id
        , e.user_email
      	, e.timestamp
    	, e.organisation_id
    	, mo.product_family
    from
      {{ ref('customers_events') }} as e      
  	  join {{ source('eh_product', 'module_ownership') }} mo on
        mo.event_module = e.module
  		and mo.product_family is not null
    where      
      e.organisation_id is not null
      and e.module != 'mobile'
      and e.module != 'others'
      and e.module != 'Sign In'
   	  and "timestamp" < (select date_trunc('day', max("timestamp")) from {{ ref('customers_events') }})
  )
  , product_family_org_dau as (
    select
      DATE_TRUNC('day', e.timestamp) as "date"
      , e.organisation_id
      , e.product_family
      , count(distinct coalesce(e.user_id, e.user_email)) as daily_users
    from
      events as e
    group by 1,2,3
  )
  , product_family_org_mau as (
    select
      dates.date
      , e.organisation_id
      , e.product_family
      , count(distinct coalesce(e.user_id, e.user_email)) as monthly_users
    from
      dates
      join events as e on
        e.timestamp < dateadd(day, 1, dates.date)
        and e.timestamp > dateadd(day, -29, dates.date)
    group by 1,2,3
  )

select
  m.date
  , m.organisation_id
  , m.product_family            	
  , coalesce(daily_users, 0) as daily_users
  , monthly_users
  , coalesce(daily_users, 0) / monthly_users :: float as dau_mau
from
  product_family_org_mau m
  left join product_family_org_dau d on
    m.date = d.date
    and m.organisation_id = d.organisation_id
    and m.product_family = d.product_family