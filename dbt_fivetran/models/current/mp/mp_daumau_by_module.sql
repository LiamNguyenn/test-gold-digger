{{
    config(
        materialized='incremental',
        alias='daumau_by_module'
    )
}}

with dates as (
select
          DATEADD('day', -generated_number::int, (current_date + 1)) date
        from ({{ dbt_utils.generate_series(upper_bound=90) }})
          where "date" < (select date_trunc('day', max("timestamp")) from {{ ref('customers_events') }})
{% if is_incremental() %}           
        and date > (select max(date) from {{this}})
{% endif %}          
)
                               
, module_events as (
        select
          e.user_id, e.timestamp, e.module
        from
          {{ ref('customers_events') }} as e
          join {{ source('postgres_public', 'users') }} u on
            u.uuid = e.user_id
          join {{ source('postgres_public', 'members') }} m on
            m.user_id = u.id
        where
          u.email !~* '.*(employmenthero|employmentinnovations|thinkei|p2m|power2motivate|test|demo|exacc|sandbox|\\+).*'
          and not u._fivetran_deleted
          and not u.is_shadow_data
          and not m.system_manager
          and not m.system_user
          --and not m.independent_contractor
          and not m._fivetran_deleted
          and not m.is_shadow_data
          and e.module != 'mobile'
          and e.module != 'others'
          and e.module != 'Sign In'
          -- bandaid solution while dev effort is being made to update the MP implementation; the next two where clause should prevent double counting
          and e.name not in ('Exit Interview - Click Use Template', 'View Custom Survey Page')
          and e.module not in ('Custom Survey', 'Exit Interview')
          and e.timestamp < (select date_trunc('day', max("timestamp")) from {{ ref('customers_events') }})

        -- bandaid solution while dev effort is being made to update the MP implementation for custom survey and exit interview
        union 

        select
          e.user_id, e.timestamp, e.sub_module as module
        from
          {{ ref('customers_events') }} as e
          join {{ source('postgres_public', 'users') }} u on
            u.uuid = e.user_id
          join {{ source('postgres_public', 'members') }} m on
            m.user_id = u.id
        where
          u.email !~* '.*(employmenthero|employmentinnovations|thinkei|p2m|power2motivate|test|demo|exacc|sandbox|\\+).*'
          and not u._fivetran_deleted
          and not u.is_shadow_data
          and not m.system_manager
          and not m.system_user
          --and not m.independent_contractor
          and not m._fivetran_deleted
          and not m.is_shadow_data
          -- bandaid solution while dev effort is being made to update the MP implementation
          and e.name in ('Exit Interview - Click Use Template', 'View Custom Survey Page')
          and e.timestamp < (select date_trunc('day', max("timestamp")) from {{ ref('customers_events') }})       
      )
      , module_dau as (
        select
          DATE_TRUNC('day', e.timestamp) as "date"
          , e.module
          , count(distinct e.user_id) as daily_users
        from
          module_events as e
        group by
          1
          , 2
      )
      , module_mau as (
        select
          dates.date
          , e.module
          , count(distinct e.user_id) as monthly_users
        from
          dates
          join module_events as e on
          e.timestamp < dateadd(day, 1, dates.date)
          and e.timestamp > dateadd(day, -29, dates.date)
        group by
          1
          , 2
      )

select
      m.date
      , m.module
      , coalesce(daily_users, 0) as daily_users
      , monthly_users
      , coalesce(daily_users, 0) / monthly_users :: float as dau_mau
    from
      module_mau m
      left join module_dau d on
        m.date = d.date
        and m.module = d.module