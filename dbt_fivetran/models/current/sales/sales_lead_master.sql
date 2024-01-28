{{ config(alias='lead_master') }}

select distinct
    l.id,
    l.lead_source_type_c,
    l.lead_source,
    l.status,
    l.country,
    l.became_mql_date,
    l.requested_demo_date_time,
    l.converted_date,
    l.sal_date,
    l.lost_reason_c,
    l.mql_score_c,
    l.sql_score_c,
    l.sal_score_c,
    rt.name    as record_type,
    u.name     as owner_name,
    p.name     as owner_profile,
    u.market_c as owner_market,
    u.manager_name
from
    (
        select distinct
            id,
            owner_id,
            record_type_id,
            lead_source_type_c,
            lead_source,
            (case lower(country)
                when 'united kingdom' then 'United Kingdom'
                when 'australia' then 'Australia'
                when 'singapore' then 'Singapore'
                when 'malaysia' then 'Malaysia'
                when 'new zealand' then 'New Zealand'
                else 'Other'
            end)                                     as country,
            cast(became_mql_date_c as date)          as became_mql_date,
            cast(sal_date_c as date)                 as sal_date,
            cast(requested_demo_date_time_c as date) as requested_demo_date_time,
            cast(converted_date as date)             as converted_date,
            lost_reason_c,
            status,
            mql_score_c,
            sql_score_c,
            sal_score_c
        --AND cast (became_mql_date_c as date) became_mql_date >= cast('2023-04-01' AS date)
        from {{ source('salesforce', 'lead') }}
        where
            is_deleted = FALSE
            and _fivetran_deleted = FALSE
    ) as l
--record type
left join
    (select distinct
        id,
        name
    from {{ source('salesforce', 'record_type') }}
    where _fivetran_deleted = FALSE)
    as rt on l.record_type_id = rt.id
-- get owner info
left join
    (
        select distinct
            u1.id,
            u1.profile_id,
            case when u1.is_active = TRUE then u1.name else 'Inactive' end as name,
            u1.market_c,
            case when u2.is_active = TRUE then u2.name else 'Inactive' end as manager_name
        from
            {{ source('salesforce', 'user') }} as u1
        -- get manager info
        left join
            {{ source('salesforce', 'user') }} as u2
            on
                u1.manager_id = u2.id
                and u2._fivetran_deleted = FALSE
        where u1._fivetran_deleted = FALSE
    ) as u on l.owner_id = u.id
--get profile info
left join
    (
        select distinct
            id,
            name
        from
            {{ source('salesforce', 'profile') }}
        where _fivetran_deleted = FALSE
    ) as p on u.profile_id = p.id
