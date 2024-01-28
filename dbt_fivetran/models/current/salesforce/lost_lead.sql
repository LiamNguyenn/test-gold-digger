with lost_organisations as (
    select
        id,
        first_name,
        last_name,
        name,
        title,
        company,
        currency_iso_code,
        country,
        email,
        website,
        description,
        industry,
        industry_primary_c,
        industry_secondary_c,
        number_of_employees,
        annual_revenue,
        created_date,
        last_modified_date,
        last_activity_date
    from {{ source('salesforce', 'lead') }}
    where
        is_deleted = FALSE
        and status = 'Lost'
),

current_organisations as (
    select
        name,
        omop_org_id
    from {{ ref('one_platform_organisations') }}
    where
        (eh_sub_name not ilike '%demo%' or eh_sub_name is NULL)
        and eh_churn_date is NULL
)

select
    lo.id,
    lo.first_name,
    lo.last_name,
    lo.name,
    lo.title,
    lo.company,
    lo.currency_iso_code,
    lo.country,
    lo.email,
    lo.website,
    lo.description,
    lo.industry,
    case when lo.industry_primary_c = 'Unknown' then NULL else lo.industry_primary_c end,
    case when lo.industry_secondary_c = 'Unknown' then NULL else lo.industry_secondary_c end,
    lo.number_of_employees,
    lo.annual_revenue,
    lo.created_date,
    lo.last_modified_date,
    lo.last_activity_date
from lost_organisations as lo
left join current_organisations as co on lo.company = co.name
where co.omop_org_id is NULL
