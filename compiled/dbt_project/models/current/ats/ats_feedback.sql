

-- with feedback_1 as (
--     select org_uuid, org_id, user_uuid, matched_job_title
--     , listagg(distinct vote, '; ') WITHIN GROUP (order by created_at desc) as votes
--     , listagg(distinct explanation, '; ') WITHIN GROUP (order by created_at desc) as explanations
--     from (        
--         select fb.id, fb.created_at, explanation, vote    
--         , u.id as feedback_user_id

--         , o.uuid as org_uuid
--         , o.id as org_id
--         , json_extract_path_text(context, 'matching_job_title')::varchar as matched_job_title
--         from "dev"."ats_public"."feedbacks" fb
--         join "dev"."postgres_public"."users" u on fb.user_id = u.uuid
--         join "dev"."employment_hero"."employees" e on e.user_id = u.id
--         join "dev"."employment_hero"."organisations" o on e.organisation_id = o.id
--         where not fb._fivetran_deleted
--         --and explanation is not null
--         and feature = 'candidate_recommendation'
--         and is_paying_eh
--     )
--     group by 1,2,3,4
--   )

with
feedback_init as (
    select
        id,
        user_id,
        created_at,
        explanation,
        vote,
        context,
        json_extract_path_text(context, 'matching_job_title')::varchar as matched_job_title,
        json_extract_path_text(context, 'user_ids')                    as context_user_ids,
        json_extract_path_text(context, 'href')                        as hrefs
    from "dev"."ats_public"."feedbacks"
    where not _fivetran_deleted and feature = 'candidate_recommendation'
)
,

numbers as (
    select generated_number::int as num
    from (

    

    with p as (
        select 0 as generated_number union all select 1
    ), unioned as (

    select

    
    p0.generated_number * power(2, 0)
     + 
    
    p1.generated_number * power(2, 1)
     + 
    
    p2.generated_number * power(2, 2)
     + 
    
    p3.generated_number * power(2, 3)
     + 
    
    p4.generated_number * power(2, 4)
     + 
    
    p5.generated_number * power(2, 5)
     + 
    
    p6.generated_number * power(2, 6)
     + 
    
    p7.generated_number * power(2, 7)
     + 
    
    p8.generated_number * power(2, 8)
     + 
    
    p9.generated_number * power(2, 9)
    
    
    + 1
    as generated_number

    from

    
    p as p0
     cross join 
    
    p as p1
     cross join 
    
    p as p2
     cross join 
    
    p as p3
     cross join 
    
    p as p4
     cross join 
    
    p as p5
     cross join 
    
    p as p6
     cross join 
    
    p as p7
     cross join 
    
    p as p8
     cross join 
    
    p as p9
    
    

    )

    select *
    from unioned
    where generated_number <= 1000
    order by generated_number

)
),

feedback_1 as (
    select
        fb.id,
        fb.created_at,
        fb.explanation,
        fb.vote,
        u.id                                                                                                                                                                       as feedback_user_id,
        fb.matched_job_title,
        fb.context_user_ids,
        case when fb.hrefs ilike '%/organisations/%' then (split_part(regexp_substr(fb.hrefs, '/organisations/([0-9]+)/', 1, 1, 'i'), '/', 3))::integer end                        as org_id,
        case when fb.hrefs ilike '%/memberships/%' then replace(split_part(regexp_substr(fb.hrefs, '/memberships/([0-9]+)/', 1, 1, 'i'), '/', 3), ' ', '') end                     as member_id1,
        case when fb.hrefs ilike '%/memberships/%' then replace(split_part(split_part(regexp_substr(fb.hrefs, '/memberships/([0-9]+)#', 1, 1, 'i'), '/', 3), '#', 1), ' ', '') end as member_id2,
        case when fb.hrefs ilike '%/memberships/%' then replace(split_part(split_part(regexp_substr(fb.hrefs, '/memberships/([0-9]+)?', 1, 1, 'i'), '/', 3), '?', 1), ' ', '') end as member_id3
    from feedback_init as fb
    inner join "dev"."postgres_public"."users" as u on fb.user_id = u.uuid
)
,
feedback_2 as (
    select
        a.id,
        a.created_at,
        a.explanation,
        a.vote,
        a.feedback_user_id,
        a.matched_job_title,
        a.context_user_ids,
        coalesce(a.org_id, b.organisation_id) as final_org_id
    from
        (select
            id,
            created_at,
            explanation,
            vote,
            feedback_user_id,
            matched_job_title,
            context_user_ids,
            org_id,
            coalesce(nullif(member_id1, ''), nullif(member_id2, ''), nullif(member_id3, '')) as member_id_final
        from feedback_1) as a
    left join "dev"."postgres_public"."members" as b on a.member_id_final = b.id and a.org_id is NULL
)
,
feedback_3 as (
    select
        a.id                                                                                          as feedbacks_id,
        a.created_at,
        a.explanation,
        a.vote,
        a.feedback_user_id,
        a.matched_job_title,
        a.final_org_id,
        o.uuid                                                                                        as org_uuid,
        replace(trim(both '[]''"' from split_part(a.context_user_ids, ',', n.num::integer)), '"', '') as user_uuid
    from feedback_2 as a
    inner join "dev"."employment_hero"."employees" as e on a.feedback_user_id = e.user_id and a.final_org_id = e.organisation_id
    inner join "dev"."employment_hero"."organisations" as o on a.final_org_id = o.id
    inner join numbers as n on n.num <= 1 + length(a.context_user_ids) - length(replace(a.context_user_ids, ',', ''))
    where
        a.context_user_ids is not NULL
        and a.context_user_ids != '[]'
        and replace(trim(both '[]''"' from split_part(a.context_user_ids, ',', n.num::integer)), '"', '') is not NULL
        and o.is_paying_eh
)


select
    org_uuid,
    final_org_id                                                                as org_id,
    user_uuid,
    matched_job_title,
    listagg(distinct vote, '; ') within group (order by created_at desc)        as votes,
    listagg(distinct explanation, '; ') within group (order by created_at desc) as explanations
from feedback_3
group by 1, 2, 3, 4