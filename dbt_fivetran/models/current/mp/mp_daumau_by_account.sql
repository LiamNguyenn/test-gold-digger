{{
    config(
        materialized='incremental',
        alias='daumau_by_account'
    )
}}

with
    dates as (
        select dateadd('day', -generated_number::int, (current_date + 1)) date
        from ({{ dbt_utils.generate_series(upper_bound=90) }})
        where
            "date" < (select date_trunc('day', max("timestamp")) from {{ ref("customers_events") }})
            {% if is_incremental() %} and date > (select max(date) from {{ this }}) {% endif %}
    ),
    account_events as (
        select e.user_id, e.timestamp, u.account_list as account_id
        from {{ ref("customers_events") }} as e
        join {{ ref("customers_users") }} u on u.user_uuid = e.user_id
        -- TODO: Fix circular reference with customers.accounts
        join {{ source("salesforce", "account") }} a on u.account_list = a.id
        where e.timestamp < (select date_trunc('day', max("timestamp")) from {{ ref("customers_events") }})
    ),
    account_dau as (
        select date_trunc('day', e.timestamp) as "date", e.account_id, count(distinct e.user_id) as daily_users
        from account_events as e
        group by 1, 2
    ),
    account_mau as (
        select dates.date, e.account_id, count(distinct e.user_id) as monthly_users
        from dates
        join
            account_events as e
            on e.timestamp < dateadd(day, 1, dates.date)
            and e.timestamp > dateadd(day, -29, dates.date)
        group by 1, 2
    )

select
    account_mau.date,
    account_mau.account_id,
    coalesce(daily_users, 0) as daily_users,
    coalesce(monthly_users, 0) as monthly_users,
    coalesce(daily_users, 0) / nullif(monthly_users, 0)::float as dau_mau
from account_mau
left join account_dau on account_mau.date = account_dau.date and account_mau.account_id = account_dau.account_id
