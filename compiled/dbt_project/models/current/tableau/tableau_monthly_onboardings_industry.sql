

select industry, date_trunc('month', ob.created_at) as onboarded_month, o.country, count(*)
from "dev"."employment_hero"."employees" as m
join "dev"."employment_hero"."organisations" o
    on m.organisation_id = o.id
    and o.id not in (select id from ats.spam_organisations)  -- remove SPAM organisations
join "dev"."postgres_public"."onboarding_infos" ob
    on ob.member_id = m.id
    and not ob._fivetran_deleted
    and ob.from ~ 'Onboarding|JobAdder'
WHERE industry is not null and onboarded_month is not null
group by 1, 2, 3
order by industry, onboarded_month, o.country