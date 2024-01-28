{{
    config(
        materialized='incremental',
        alias='companydash_mmau_by_platform'
    )
}}

with
    dates as (
        select dateadd('day', -generated_number::int, (current_date + 1)) date
        from ({{ dbt_utils.generate_series(upper_bound=14) }})
        where
            "date" < (select date_trunc('day', max("timestamp")) from {{ ref("customers_events") }})
            {% if is_incremental() %}
              and date > (select max(date) from {{ this }} )
            {% endif %}
    )

select
    dates.date,
    (
        case
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
    ) as country,
    count(
        distinct case when app_version_string is not null then coalesce(e.user_id, e.user_email) end
    ) as mobile_app_mau,
    count(distinct case when app_version_string is null then coalesce(e.user_id, e.user_email) end) as web_browser_mau,
    count(
        distinct case when app_version_string is not null and o.is_paying_eh then coalesce(e.user_id, e.user_email) end
    ) as mobile_app_mmau,
    count(
        distinct case when app_version_string is null and o.is_paying_eh then coalesce(e.user_id, e.user_email) end
    ) as web_browser_mmau
from dates

inner join {{ ref("customers_events") }} as e 
    on e.timestamp < dateadd(day, 1, dates.date) and e.timestamp >= dateadd(day, -89, dates.date)

left join {{ ref("employment_hero_employees") }} as ee 
    on ee.uuid = e.member_uuid

left join {{ ref("employment_hero_organisations") }} as o 
    on ee.organisation_id = o.id

left join {{ source("eh_product", "module_ownership") }} as mo 
    on mo.event_module = e.module

group by 1, 2
