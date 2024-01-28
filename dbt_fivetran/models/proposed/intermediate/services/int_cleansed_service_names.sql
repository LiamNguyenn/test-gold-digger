with pingdom_services as (
    select distinct
        'pingdom'                                                                      as platform,
        name,
        replace(replace(translate(lower(name), ' ', '-'), '-service', ''), '-api', '') as cleansed_name

    from {{ ref("stg_workshop_public__pingdom_checks") }}
),

prometheus_services as (
    select distinct
        'prometheus' as platform,
        name,
        case
            when name ilike 'application.%' then replace(replace(replace(split_part(name, '.', 2), '-web', ''), '-api', ''), '-service', '')
            else replace(replace(replace(replace(split_part(name, '.application', 1), '-web', ''), '-api', ''), '-service', ''), '-rpc', '')
        end          as cleansed_name

    from {{ ref("stg_workshop_public__prometheus_services") }}
    where name !~* '^destination_app:.'
),

datadog_services as (
    select
        'datadog'                                                                 as platform,
        name,
        regexp_replace(split_part(name, ':', 2), '(-service|-web|-api|-rpc)', '') as cleansed_name

    from {{ ref("stg_workshop_public__prometheus_services") }}
    where name ~* '^destination_app:.'
    group by 1, 2, 3
),

sentry_services as (
    select distinct
        'sentry'                                                                                        as platform,
        project_name                                                                                    as name, -- noqa: RF04
        replace(replace(replace(lower(project_name), '-api', ''), '-service', ''), 'eh-sandbox', 'sbx') as cleansed_name

    from {{ ref("stg_eh_infra_stat_service_raw__daily_report_sentry_issues") }}
),

checkly as (
    select distinct
        'checkly' as platform,
        name,
        name      as cleansed_name
    from {{ ref("stg_checkly__all_checks") }}
)

select * from pingdom_services
union all
select * from prometheus_services
union all
select * from datadog_services
union all
select * from sentry_services
union all
select * from checkly
