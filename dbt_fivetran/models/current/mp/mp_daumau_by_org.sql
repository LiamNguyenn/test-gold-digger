{{
    config(
        materialized='incremental',
        alias='daumau_by_org'
    )
}}

with
  dates as (
    select
      DATEADD('day', -generated_number::int, (current_date + 1)) date
    from ({{ dbt_utils.generate_series(upper_bound=365) }})
      where "date" < (select date_trunc('day', max("timestamp")) from {{ ref('customers_events') }})
{% if is_incremental() %}           
      and date > (select max(date) from {{this}})
{% endif %}  
  )
  , sessions as (
    select distinct
      date session_date
      , m.organisation_id
      , m.id member_id
    from
      mp.daily_members d
      join {{ source('postgres_public', 'members') }} m on
        d.member_id = m.id
      join {{ source('postgres_public', 'users') }} u on
        m.user_id = u.id
    where
      u.email !~* '.*(employmenthero|employmentinnovations|thinkei|p2m|power2motivate|test|demo|exacc|sandbox|\\+).*'
      and not m.system_manager
      and not m.system_user
      and not m._fivetran_deleted
      and not m.is_shadow_data
      and not u.is_shadow_data
      and "date" < (select date_trunc('day', max("timestamp")) from {{ ref('customers_events') }})      

  )
  , dau as (
    select
      d.date
      , organisation_id
      , count(*) daily_users
    from
      dates d
      left join sessions s on 
        d.date = s.session_date
    group by
      d.date
      , organisation_id
  )
  , mau as (
    select
      d.date
      , s.organisation_id
      , count(distinct s.member_id) monthly_users
    from
      dates d
      left join sessions s on
        s.session_date < dateadd(day, 1, d.date)
        and s.session_date > dateadd(day, -29, d.date)
      group by
        d.date
        , s.organisation_id
  )
select
  mau.date
  , mau.organisation_id
  , coalesce(daily_users, 0) as daily_users
  , coalesce(monthly_users, 0) as monthly_users
  , coalesce(daily_users, 0) / nullif(monthly_users, 0)::float as dau_mau
from
  mau 
  left join dau on
    mau.date = dau.date
    and mau.organisation_id = dau.organisation_id