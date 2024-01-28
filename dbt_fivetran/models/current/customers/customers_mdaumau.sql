{{
    config(
        materialized='incremental',
        alias='mdaumau'
    )
}}

with dates as (
select
          DATEADD('day', -generated_number::int, (current_date + 1)) date
        from ({{ dbt_utils.generate_series(upper_bound=365) }})   
          where "date" < (select date_trunc('day', max("timestamp")) from {{ ref('customers_events') }})
{% if is_incremental() %}
        and "date" > (select max("date") from {{ this }})
{% endif %}
)
                               
, events as (
        select
          e.user_id, e.timestamp
        from
          {{ ref('customers_events') }} as e 
  		  join {{ ref('customers_users') }} u on e.user_id = u.user_uuid
		join {{ ref('customers_accounts') }} a on u.account_list = a.external_id
        where a.account_stage != 'Churned'
        and e.timestamp < (select date_trunc('day', max("timestamp")) from {{ ref('customers_events') }})
      )
      , mdau as (
        select
          DATE_TRUNC('day', e.timestamp) as "date"
          , count(distinct e.user_id) as daily_users
        from
          events as e   
        {% if is_incremental() %}     
        where e.timestamp > dateadd('day', 1, (select max("date") from {{ this }}))
        {% endif %}
        group by
          1
      )
      , mmau as (
        select
          dates.date
          , count(distinct e.user_id) as monthly_users
        from
          dates
          join events as e on
          e.timestamp < dateadd(day, 1, dates.date)
          and e.timestamp > dateadd(day, -29, dates.date)
        group by
          1
      )

select
      m.date
      , coalesce(daily_users, 0) as mdau
      , monthly_users as mmau
      , coalesce(mdau, 0) / mmau :: float as mdau_mau
    from
     mmau m
      left join mdau d on
        m.date = d.date