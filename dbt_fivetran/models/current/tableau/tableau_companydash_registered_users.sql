{{
    config(
        materialized='incremental',
        alias='companydash_registered_users'
    )
}}

with dates as (
    select
          DATEADD('day', -generated_number::int, (current_date + 1)) as date
        from ({{ dbt_utils.generate_series(upper_bound=90) }})
          where "date" < current_date
          {% if is_incremental() %}
            and date > (select max(date) from {{ this }} )
          {% endif %}
            
)
, employees as (
    select user_uuid
      , ( case
          when lower(work_country) = 'au'
            then 'Australia'
          when lower(work_country) = 'gb'
            then 'United Kingdom'
          when lower(work_country) = 'sg'
            then 'Singapore'
          when lower(work_country) = 'my'
            then 'Malaysia'
          when lower(work_country) = 'nz'
            then 'New Zealand'
          else 'untracked'
        end
      )
      as country
      , min(created_at::date) as first_registered_date

    from {{ ref("employment_hero_employees") }}

    group by 1, 2
)
, app_downloads as (
    select ee.user_uuid
        , ( case
          when lower(ee.work_country) = 'au'
            then 'Australia'
          when lower(ee.work_country) = 'gb'
            then 'United Kingdom'
          when lower(ee.work_country) = 'sg'
            then 'Singapore'
          when lower(ee.work_country) = 'my'
            then 'Malaysia'
          when lower(ee.work_country) = 'nz'
            then 'New Zealand'
          else 'untracked'
        end
      )
      as country
        , min(timestamp::date) as first_app_signin_date

    from {{ ref("customers_events") }} ce

    inner join {{ ref("employment_hero_employees") }} ee
      on ee.uuid = ce.member_uuid

    where name ~~* '%sign in%'
      and platform ~~* '%mobile%'
    
    group by 1, 2)

, registered_users as (
    select first_registered_date
      , country
      , count(distinct user_uuid) as registered_users

    from employees

    group by 1, 2
)

, registered_app_users as (
    select first_app_signin_date
      , country
      , count(distinct user_uuid) as registered_app_users

    from app_downloads

    group by 1, 2
)
, cumulative_users as (
    select d.date
        , country
        , sum(registered_users) as total_registered_users

    from dates d

    left join registered_users e
        on d.date >= e.first_registered_date

    group by 1, 2

    order by 1
)
, cumulative_app_users as (
    select d.date
        , country
        , sum(registered_app_users) as total_registered_app_users

    from dates d

    left join registered_app_users a
        on d.date >= a.first_app_signin_date

    group by 1, 2

    order by 1
)
, total as (
  select "date"
    , country
    , 'Total' as type
    , total_registered_users::int as users

  from cumulative_users

  union 

  select "date"
    , country
    , 'App' as type
    , total_registered_app_users::int as users

  from cumulative_app_users
)

select date
  , total::int as total_registered_users
  , app::int as total_registered_app_users
  , country

from total
  pivot (sum(users) for type in ('Total', 'App'))

order by 1