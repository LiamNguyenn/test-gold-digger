{{ config(alias='feedback') }}

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
--         from {{source('ats_public', 'feedbacks')}} fb
--         join {{source('postgres_public', 'users')}} u on fb.user_id = u.uuid
--         join {{ref('employment_hero_employees')}} e on e.user_id = u.id
--         join {{ref('employment_hero_organisations')}} o on e.organisation_id = o.id
--         where not fb._fivetran_deleted
--         --and explanation is not null
--         and feature = 'candidate_recommendation'
--         and is_paying_eh
--     )
--     group by 1,2,3,4
--   )

with 
feedback_init 
as 
(
select id
, user_id
, created_at
, explanation
, vote
, context
, json_extract_path_text(context, 'matching_job_title')::varchar as matched_job_title
, json_extract_path_text(context, 'user_ids') as context_user_ids
, json_extract_path_text(context, 'href') as hrefs
from {{source('ats_public', 'feedbacks')}}
where not _fivetran_deleted and feature = 'candidate_recommendation'
)
,

Numbers AS (
  SELECT generated_number::int AS num
  FROM ({{ dbt_utils.generate_series(upper_bound=1000) }})
)

,feedback_1 as
(select
 fb.id
,fb.created_at
,fb.explanation
,fb.vote
,u.id as feedback_user_id
,fb.matched_job_title
,fb.context_user_ids
,Case when fb.hrefs ILIKE '%/organisations/%' then CAST(split_part(regexp_substr(fb.hrefs, '/organisations/([0-9]+)/', 1, 1, 'i'), '/', 3) AS INTEGER) else null end AS Org_Id
,Case when fb.hrefs ILIKE '%/memberships/%' then replace(split_part(regexp_substr(fb.hrefs, '/memberships/([0-9]+)/', 1, 1, 'i'), '/', 3), ' ', '') else null end as member_id1
,Case when fb.hrefs ILIKE '%/memberships/%' then replace(split_part(split_part(regexp_substr(fb.hrefs, '/memberships/([0-9]+)#', 1, 1, 'i'), '/', 3), '#', 1), ' ', '') else null end as member_id2
,Case when fb.hrefs ILIKE '%/memberships/%' then replace(split_part(split_part(regexp_substr(fb.hrefs, '/memberships/([0-9]+)?', 1, 1, 'i'), '/', 3), '?', 1), ' ', '') else null end as member_id3
from feedback_init fb
join {{source('postgres_public', 'users')}} u on fb.user_id = u.uuid
)
,
feedback_2 as
(
select 
  a.id
,a.created_at
,a.explanation
,a.vote
,a.feedback_user_id
,a.matched_job_title
,a.context_user_ids
,COALESCE(a.org_id, b.organisation_id) as final_org_id
from
(select 
 id
,created_at
,explanation
,vote
,feedback_user_id
,matched_job_title
,context_user_ids
,Org_id
, COALESCE(NULLIF(member_id1, ''), NULLIF(member_id2, ''), NULLIF(member_id3, '')) as member_id_final 
 from feedback_1) a
left join {{source('postgres_public', 'members')}} b on a.member_id_final = b.id and a.org_id is null
)
,
feedback_3 as
(
select 
 a.id as feedbacks_id
,a.created_at
,a.explanation
,a.vote
,a.feedback_user_id
,a.matched_job_title
,a.final_org_id
,o.uuid as org_uuid
,REPLACE(TRIM(BOTH '[]''"' FROM split_part(a.context_user_ids, ',', n.num::integer)), '"', '') as user_uuid
from feedback_2 a
join {{ref('employment_hero_employees')}} e on a.feedback_user_id = e.user_id and a.final_org_id = e.organisation_id
join {{ref('employment_hero_organisations')}} o on a.final_org_id = o.id
JOIN Numbers n ON n.num <= 1 + LENGTH(a.context_user_ids) - LENGTH(REPLACE(a.context_user_ids, ',', ''))
where a.context_user_ids IS NOT NULL 
and a.context_user_ids != '[]'
and REPLACE(TRIM(BOTH '[]''"' FROM split_part(a.context_user_ids, ',', n.num::integer)), '"', '') IS NOT NULL
and o.is_paying_eh
)


select org_uuid, final_org_id as org_id, user_uuid, matched_job_title
    , listagg(distinct vote, '; ') WITHIN GROUP (order by created_at desc) as votes
    , listagg(distinct explanation, '; ') WITHIN GROUP (order by created_at desc) as explanations
from feedback_3
group by 1, 2, 3, 4

