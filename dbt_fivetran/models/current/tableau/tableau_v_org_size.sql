{{ config(materialized="view", alias="v_org_size") }}

WITH
    dates as (
        select
            dateadd(
                'month', - n, (date_trunc('month', add_months(current_date, 1)))
            )::date date
        from
            (
                select row_number() over () as n
                from {{ ref("customers_accounts") }}
                limit 300
            -- just generating n which is a number between 1-300
            ) as n
        where date >= '2017-01-01'
    ),
    org_size as (
        select
            d.date,
            organisation_id,
            count(*) as total_employees,
            case
                when total_employees < 20
                then '1-19'
                when total_employees between 20 and 199
                then '20-199'
                when total_employees > 200
                then '200+'
            end as company_size
        from dates d
        join
            {{ ref("employment_hero_employees") }} as e
            on e.start_date <= d.date
            and (e.termination_date >= d.date or e.termination_date is null)
            and e.created_at <= d.date
        join
            {{ ref("employment_hero_organisations") }} as o
            on o.id = e.organisation_id
            and o.created_at <= d.date
        where  -- e.active and
            o.pricing_type != 'demo'
        group by 1, 2
    )
SELECT * FROM org_size