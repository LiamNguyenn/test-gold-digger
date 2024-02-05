

with
converted as (
    select
        organisation_id,
        converted_at,
        pricing_tier as first_paid_tier
    from (
        select
            a.organisation_id,
            a.created_at                                                             as converted_at,
            s.pricing_tier,
            row_number() over (partition by a.organisation_id order by a.created_at) as rn
        from
            "dev"."postgres_public"."agreements" as a  -- noqa: AL06
        inner join "dev"."employment_hero"."_v_sub_plan_grouping" as s on a.subscription_plan_id = s.id  -- noqa: AL06
        where
            not a._fivetran_deleted
            and s.pricing_tier != 'free'
    )
    where rn = 1
),

salesforce_eh_orgs as (
    select
        eh_org_c.id as sf_eh_org_id,
        o.*
    from
        "dev"."salesforce"."eh_org_c"
    inner join "dev"."employment_hero"."organisations" as o on eh_org_c.org_id_c = o.id  -- noqa: AL06
    where
        not eh_org_c.is_deleted
),

employees as (
    select
        m.email,
        m.organisation_id as org_id,
        m.org_name,
        m.sub_name,
        o.created_at      as org_created_at
    from
        "dev"."employment_hero"."employees" as m  -- noqa: AL06
    inner join "dev"."postgres_public"."organisations" as o on m.organisation_id = o.id and not o._fivetran_deleted and not o.is_shadow_data  -- noqa: AL06
)


select distinct
    l.id                                                                                                                                                                 as lead_id,
    l.created_date                                                                                                                                                       as lead_created_date,
    l.status                                                                                                                                                             as lead_status,
    l.company                                                                                                                                                            as lead_company,
    coalesce(l.industry_primary_c, 'Unknown')                                                                                                                            as lead_industry,
    l.number_of_employees                                                                                                                                                as lead_num_of_employees,
    l.country                                                                                                                                                            as lead_country,
    l.name                                                                                                                                                               as lead_name,
    l.email                                                                                                                                                              as lead_email,
    l.title                                                                                                                                                              as lead_title,
    l.lost_reason_c                                                                                                                                                      as lead_lost_reason,
    l.lost_sub_reason_c                                                                                                                                                  as lead_lost_sub_reason,
    l.lost_reason_detail_c                                                                                                                                               as lead_lost_reason_detail,
    l.most_recent_conversion_c                                                                                                                                           as campaign,
    l.date_assigned_to_owner_c                                                                                                                                           as date_assigned_to_owner,
    l.most_recent_sales_contact_c                                                                                                                                        as most_recent_sales_contact,
    coalesce(o.id, m.org_id)                                                                                                                                             as organisation_id,
    case
        when o.id is not NULL then o.name
        when o.id is NULL and m.org_id is not NULL then m.org_name
    end                                                                                                                                                                  as eh_organisation_name,
    case
        when o.id is not NULL then o.created_at
        when o.id is NULL and m.org_id is not NULL then m.org_created_at
    end                                                                                                                                                                  as opportunity_date,
    case
        when o.id is not NULL then o.sub_name
        when o.id is NULL and m.org_id is not NULL then m.sub_name
    end                                                                                                                                                                  as eh_subscription,
    c.converted_at                                                                                                                                                       as conversion_date,
    coalesce(eh_subscription ilike '%CSA%' or eh_subscription ilike '%Reseller%' or lead_lost_reason ilike '%Unqualified%' or lead_lost_reason ilike 'Authority', FALSE) as unqualified,
    coalesce(eh_subscription ilike '%free%', FALSE)                                                                                                                      as opportunity,
    coalesce(eh_subscription ilike '%Zuora%', FALSE)                                                                                                                     as close_won,
    coalesce(eh_subscription is NULL, FALSE)                                                                                                                             as bad_lead

from
    "dev"."salesforce"."lead" as l  -- noqa: AL06
-- owner must be for Organics, currently only Wylie, l.owner_id = '0055h000000ae0iAAA'
inner join "dev"."salesforce"."user" as u on l.owner_id = u.id and u.market_c = 'Organic' and not u._fivetran_deleted  -- noqa: AL06
-- if eh_org exist in salesforce, use that as it will be accurate but since its not governed, many are null
-- so if they are null then join on the email to get the org, not the most accurate but its the only other link from leads to orgs
left join salesforce_eh_orgs as o on l.eh_org_c = o.sf_eh_org_id and (o.sub_name != 'Demo' or o.sub_name is NULL)  -- noqa: AL06
left join employees as m on l.email = m.email and (m.sub_name != 'Demo' or m.sub_name is NULL)  -- noqa: AL06
left join converted as c on coalesce(o.id, m.org_id) = c.organisation_id  -- noqa: AL06
where
    l.email !~* '.*(employmenthero|employmentinnovations|thinkei|p2m|power2motivate|test|demo|exacc|sandbox|\\+).*'
    and l.number_of_employees <= 10
    and l.created_date > '2021-03-31'
order by lead_created_date desc