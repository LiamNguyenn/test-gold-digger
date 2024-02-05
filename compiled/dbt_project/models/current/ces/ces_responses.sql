

with
members_integration as (
    select
        m.id,
        m.uuid,
        replace(p.type, 'Auth', '') as integration
    from
        "dev"."postgres_public"."members" as m
    left join "dev"."employment_hero"."_v_last_connected_payroll" as p on m.organisation_id = p.organisation_id
    where
        not m.is_shadow_data
)

select distinct
    c.id,
    case
        when date_part(hour, c.created_at) > 13 then dateadd(day, 1, c.created_at)::date
        else c.created_at::date
    end                                                                                                                                                                                                                            as response_date,
    m.id                                                                                                                                                                                                                           as member_id,
    m.integration                                                                                                                                                                                                                  as payroll_integration,
    initcap(c.properties_feature)                                                                                                                                                                                                  as feature,
    c.properties_delighted_country                                                                                                                                                                                                 as country,
    case when c.properties_app_version is not NULL then 'mobile' else 'web' end                                                                                                                                                    as app_type,
    f.product,
    f.workstream,
    f.event_module                                                                                                                                                                                                                 as module,
    pf.product_family,
    c.score,
    (c.score::float / 5 * 7)::decimal(2, 0)                                                                                                                                                                                        as q1_score,
    r2.scale                                                                                                                                                                                                                       as q2_score,
    r3.scale                                                                                                                                                                                                                       as q3_score,
    r4.scale                                                                                                                                                                                                                       as q4_score,
    r5.scale                                                                                                                                                                                                                       as q5_score,
    r6.scale                                                                                                                                                                                                                       as q6_score,
    ((q1_score + coalesce(q2_score, 0) + coalesce(q3_score, 0) + coalesce(q4_score, 0) + coalesce(q5_score, 0) + coalesce(q6_score, 0))::float / nullif(count(rx.scale) over (partition by rx.response_id) + 1, 0))::decimal(2, 1) as average_score
from
    "dev"."delighted_ces"."response" as c
left join "dev"."delighted_ces"."person" as p
    on
        c.person_id = p.id
left join members_integration as m
    on
        lower(c.properties_member_uuid) = lower(m.uuid)
left join eh_product.feature_ownership as f
    on
        lower(c.properties_feature) = lower(f.feature)
left join "dev"."eh_product"."product_families" as pf
    on
        lower(f.workstream) = lower(pf.workstream)
left join "dev"."delighted_ces"."response_answer" as r1
    on
        c.id = r1.response_id
        and r1.question_id = 'text_Uo2SVL'
left join "dev"."delighted_ces"."response_answer" as r2
    on
        c.id = r2.response_id
        and r2.question_id = 'integer_TbGJrg'
left join "dev"."delighted_ces"."response_answer" as r3
    on
        c.id = r3.response_id
        and r3.question_id = 'integer_EmIYKP'
left join "dev"."delighted_ces"."response_answer" as r4
    on
        c.id = r4.response_id
        and r4.question_id = 'integer_Wny12L'
left join "dev"."delighted_ces"."response_answer" as r5
    on
        c.id = r5.response_id
        and r5.question_id = 'integer_V9RguN'
left join "dev"."delighted_ces"."response_answer" as r6
    on
        c.id = r6.response_id
        and r6.question_id = 'integer_IticGz'
left join "dev"."delighted_ces"."response_answer" as rx
    on
        c.id = rx.response_id
where
    
    p.email !~* '.*(employmenthero|employmentinnovations|keypay|webscale|thinkei|p2m|power2motivate|test|demo|exacc|sandbox|\\+).*'
