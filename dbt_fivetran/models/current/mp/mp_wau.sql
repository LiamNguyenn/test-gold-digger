{{
    config(
        materialized='incremental',
        alias='wau'
    )
}}

with dates as (
select
          DATEADD('day', -generated_number::int, (current_date + 1)) date
        from ({{ dbt_utils.generate_series(upper_bound=365) }})        
          where "date" < (select date_trunc('day', max("timestamp")) from {{ ref('customers_events') }})
{% if is_incremental() %}
        and "date" > (SELECT MAX("date") FROM {{ this }} ) 
{% endif %}        

)                               
     , wau as (
        select
          dates.date
          , count(distinct e.user_id) as weekly_users
        from
          dates join {{ ref('customers_events') }} as e on e.timestamp < dateadd(day, 1, dates.date)
          and e.timestamp > dateadd(day, -6, dates.date)
          join {{ source('postgres_public', 'users') }} u on
            u.uuid = e.user_id
          join {{ source('postgres_public', 'members') }} m on
            m.user_id = u.id
        where
          u.email !~* '.*(employmenthero|employmentinnovations|thinkei|p2m|power2motivate|test|demo|exacc|sandbox|\\+).*'
          and not u._fivetran_deleted
          and not u.is_shadow_data
          and not m.system_manager
          and not m."system_user"
          --and not m.independent_contractor
          and not m._fivetran_deleted
          and not m.is_shadow_data
          and e."timestamp" < (select date_trunc('day', max("timestamp")) from {{ ref('customers_events') }})          
        group by
          1
      )    
select
      w.date      
      , weekly_users      
    from
      wau w