{{ config(alias='monthly_onboardings_industry') }}

select industry, date_trunc('month', ob.created_at) as onboarded_month, o.country, count(*)
from {{ ref('employment_hero_employees') }} as m
join {{ ref('employment_hero_organisations') }} o
    on m.organisation_id = o.id
    and o.id not in (select id from ats.spam_organisations)  -- remove SPAM organisations
join {{ source('postgres_public', 'onboarding_infos') }} ob
    on ob.member_id = m.id
    and not ob._fivetran_deleted
    and ob.from ~ 'Onboarding|JobAdder'
WHERE industry is not null and onboarded_month is not null
group by 1, 2, 3
order by industry, onboarded_month, o.country
