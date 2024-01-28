{{ config(alias='smart_match_employer_actions') }}

with candidates_shown as (
  Select org_uuid
  , o.id as org_id
  , s.user_uuid
  , s.employer_member_uuid
  , job_matched 
  , min(s.time) as first_shown_at
  , count(*) as No_Of_Shown_Actions
from {{ref('ats_smart_match_candidates_shown')}} s
  join {{ref('employment_hero_organisations')}} o on s.org_uuid = o.uuid and o.is_paying_eh
  group by 1, 2, 3, 4, 5
)

, candidates_previewed as (
  Select org_uuid
  , o.id as org_id
  , s.user_uuid
  , s.employer_member_uuid
  , job_matched
  , min(s.time) as first_previewed_at
  , count(*) as No_Of_Previewed_Actions
from {{ref('ats_smart_match_candidates_previewed')}} s
  join {{ref('employment_hero_organisations')}} o on s.org_uuid = o.uuid and o.is_paying_eh
  group by 1, 2, 3, 4, 5
)

, shortlist_candidates_action as (
  Select org_uuid
  , o.id as org_id
  , s.user_uuid
  , s.employer_member_uuid
  , job_matched
  , min(s.time) as shortlisted_at
  , count(*) as No_Of_Shortlisted_Actions
from {{ref('ats_smart_match_candidates_shortlisted_actioned')}} s
  join {{ref('employment_hero_organisations')}} o on s.org_uuid = o.uuid and o.is_paying_eh
  group by 1, 2, 3, 4, 5
)

, shortlist_candidates_db_records as (
      Select distinct j.organisation_id as org_uuid
  , o.id as org_id
  , cj.user_id as user_uuid
  , null as employer_member_uuid
  , coalesce(j.title, '')::varchar as job_matched
  , min(cj.created_at) as shortlisted_at
  , count(*) as No_Of_Shortlisted_Actions
From {{source('ats_public', 'candidate_jobs')}} cj
join {{source('ats_public', 'jobs')}} as j on cj.job_id = j.id
join {{ref('employment_hero_organisations')}} o on j.organisation_id = o.uuid
left Join {{source('ats_public', 'hiring_phases')}} hp on cj.hiring_phase_id = hp.id
Where cj.source_name = 'Shortlisted from Saved candidate'
and is_paying_eh
group by 1,2,3,4,5
)

, shortlisted_candidates as (
    select coalesce(a.org_uuid, d.org_uuid) as org_uuid
    , coalesce(a.org_id, d.org_id) as org_id
    , coalesce(a.user_uuid, d.user_uuid) as user_uuid
    , coalesce(a.employer_member_uuid, d.employer_member_uuid) as employer_member_uuid
    , coalesce(case when a.job_matched = '' then null else a.job_matched end, d.job_matched)::varchar as job_matched
    , min(coalesce(a.shortlisted_at, d.shortlisted_at)) as shortlisted_at
    , max(coalesce(a.No_Of_Shortlisted_Actions, d.No_Of_Shortlisted_Actions)) as No_Of_Shortlisted_Actions
    --, a.shortlist_member_id
    from shortlist_candidates_action a 
    full outer join shortlist_candidates_db_records d 
        on d.org_uuid = a.org_uuid and d.user_uuid  = a.user_uuid 
        and (a.job_matched = d.job_matched or a.job_matched = '' or d.job_matched = '')
        and (a.employer_member_uuid = d.employer_member_uuid or a.employer_member_uuid is null or d.employer_member_uuid is null)
        and date_trunc('day', a.shortlisted_at) <= date_trunc('day', d.shortlisted_at)
    GROUP BY 1, 2, 3, 4, 5
)

, saved_candidates_action as (
  Select org_uuid
  , o.id as org_id
  , s.user_uuid
  , s.employer_member_uuid
  , job_matched
  , min(s.time) as saved_at
  , count(*) as No_Of_Saved_Actions
from {{ref('ats_smart_match_candidates_saved_actioned')}} s
  join {{ref('employment_hero_organisations')}} o on s.org_uuid = o.uuid and o.is_paying_eh
  group by 1, 2, 3, 4, 5
)

, saved_candidates_db_records as (
Select  
  o.uuid as org_uuid
  , o.id as org_id
  , u.uuid as user_uuid
  , null as employer_member_uuid
  , coalesce(sc.job_saved_for, '')::varchar as job_matched
  --, author_id as save_member_id
  , min(sc.created_at) as saved_at
  , count(*) as No_Of_Saved_Actions
  from {{source('postgres_public', 'saved_candidates')}} sc
  join {{ref('employment_hero_organisations')}} o on o.id = sc.organisation_id
  join {{source('postgres_public', 'users')}} u on u.id = sc.user_id
  where is_paying_eh
    group by 1,2,3,4,5
)

, saved_candidates as (
    select coalesce(a.org_uuid, d.org_uuid) as org_uuid
    , coalesce(a.org_id, d.org_id) as org_id
    , coalesce(a.user_uuid, d.user_uuid) as user_uuid
    , coalesce(a.employer_member_uuid, d.employer_member_uuid) as employer_member_uuid
    , coalesce(case when a.job_matched = '' then null else a.job_matched end, d.job_matched)::varchar as job_matched
    , min(coalesce(a.saved_at, d.saved_at)) as saved_at
    , max(coalesce(a.No_Of_Saved_Actions, d.No_Of_Saved_Actions)) as No_Of_Saved_Actions
    --, coalesce(a.save_member_id, d.save_member_id) as save_member_id
    from saved_candidates_action a 
    full outer join saved_candidates_db_records d
        on d.org_uuid = a.org_uuid and d.user_uuid  = a.user_uuid 
        and (a.job_matched = d.job_matched or a.job_matched = '' or d.job_matched = '')
        and (a.employer_member_uuid = d.employer_member_uuid or a.employer_member_uuid is null or d.employer_member_uuid is null)
        and date_trunc('day', a.saved_at) <= date_trunc('day', d.saved_at)
    group by 1, 2, 3, 4, 5
)

, saved_or_shortlisted_candidates as (
    select
    coalesce(sc.org_uuid, sl.org_uuid) as org_uuid
    , coalesce(sc.org_id, sl.org_id) as org_id
    , coalesce(sc.user_uuid, sl.user_uuid) as user_uuid
    , coalesce(sc.employer_member_uuid, sl.employer_member_uuid) as employer_member_uuid
    , coalesce(case when sl.job_matched = '' then null else sl.job_matched end, sc.job_matched)::varchar as job_matched
    , min(sc.saved_at) as saved_at
    , min(coalesce(sc.saved_at, sl.shortlisted_at)) as saved_or_shortlisted_at
    , max(sc.No_Of_Saved_Actions) as No_Of_Saved_Actions
    --, sc.save_member_id
    , min(sl.shortlisted_at) as shortlisted_at
    , max(sl.No_Of_Shortlisted_Actions) as No_Of_Shortlisted_Actions
    --, sl.shortlist_member_id
    from saved_candidates sc 
    full outer join shortlisted_candidates sl 
        on sc.org_uuid = sl.org_uuid and sc.user_uuid = sl.user_uuid
        and (sl.job_matched = sc.job_matched or sl.job_matched = '' or sc.job_matched = '')
        and (sl.employer_member_uuid = sc.employer_member_uuid or sl.employer_member_uuid is null or sc.employer_member_uuid is null)
        and date_trunc('day', sc.saved_at) <= date_trunc('day', sl.shortlisted_at)
    group by 1, 2, 3, 4, 5
)

, base as (
    select x.org_uuid
    , x.org_id
    , x.user_uuid
    , x.employer_member_uuid
    , x.job_matched
    --, save_member_id
    --, shortlist_member_id
    , min(first_shown_at) as first_shown_at
    , sum(No_Of_Shown_Actions) as No_Of_Shown_Actions
    , min(first_previewed_at) as first_previewed_at
    , sum(No_Of_Previewed_Actions) as No_Of_Previewed_Actions
    , min(saved_at) as saved_at
    , sum(No_Of_Saved_Actions) as No_Of_Saved_Actions
    , min(saved_or_shortlisted_at) as saved_or_shortlisted_at
    , min(shortlisted_at) as shortlisted_at
    , sum(No_Of_Shortlisted_Actions) as No_Of_Shortlisted_Actions
    from (
        select coalesce(p.org_uuid, cs.org_uuid) as org_uuid
        , coalesce(p.org_id, cs.org_id) as org_id
        , coalesce(p.user_uuid, cs.user_uuid) as user_uuid
        , coalesce(p.employer_member_uuid, cs.employer_member_uuid) as employer_member_uuid
        , coalesce(case when p.job_matched = '' then null else p.job_matched end, cs.job_matched)::varchar as job_matched
        , min(cs.first_shown_at) as first_shown_at
        , sum(cs.No_Of_Shown_Actions) as No_Of_Shown_Actions
        , min(p.first_previewed_at) as first_previewed_at
        , sum(p.No_Of_Previewed_Actions) as No_Of_Previewed_Actions
        , min(p.saved_at) as saved_at
        , sum(p.No_Of_Saved_Actions) as No_Of_Saved_Actions
        , min(p.saved_or_shortlisted_at) as saved_or_shortlisted_at
        , min(p.shortlisted_at) as shortlisted_at
        , sum(p.No_Of_Shortlisted_Actions) as No_Of_Shortlisted_Actions
        --, p.save_member_id
        --, p.shortlist_member_id
        from (
            select coalesce(s.org_uuid, p.org_uuid) as org_uuid
            , coalesce(s.org_id, p.org_id) as org_id
            , coalesce(s.user_uuid, p.user_uuid) as user_uuid
            , coalesce(s.employer_member_uuid, p.employer_member_uuid) as employer_member_uuid
            , coalesce(case when s.job_matched = '' then null else s.job_matched end, p.job_matched)::varchar as job_matched
            , min(p.first_previewed_at) as first_previewed_at
            , sum(p.No_Of_Previewed_Actions) as No_Of_Previewed_Actions
            , min(s.saved_at) as saved_at
            , sum(s.No_Of_Saved_Actions) as No_Of_Saved_Actions
            , min(s.saved_or_shortlisted_at) as saved_or_shortlisted_at
            --, s.save_member_id            
            , min(s.shortlisted_at) as shortlisted_at
            , sum(s.No_Of_Shortlisted_Actions) as No_Of_Shortlisted_Actions

            --, s.shortlist_member_id
            from saved_or_shortlisted_candidates s
            full outer join candidates_previewed p 
                on s.org_uuid = p.org_uuid and s.user_uuid = p.user_uuid 
                and (s.job_matched = p.job_matched or s.job_matched = '' or p.job_matched = '')
                and (s.employer_member_uuid = p.employer_member_uuid or s.employer_member_uuid is null or p.employer_member_uuid is null)
                and date_trunc('day', p.first_previewed_at) <= date_trunc('day', s.saved_or_shortlisted_at)
            group by 1, 2, 3, 4, 5
            )p
            full outer join candidates_shown cs
                on p.org_uuid = cs.org_uuid and p.user_uuid = cs.user_uuid 
                and (p.job_matched = cs.job_matched or p.job_matched = '' or cs.job_matched = '') 
                and (p.employer_member_uuid = cs.employer_member_uuid or p.employer_member_uuid is null or cs.employer_member_uuid is null)
                and date_trunc('day', cs.first_shown_at) <= date_trunc('day', p.first_previewed_at)
            group by 1, 2, 3, 4, 5
    ) x
    left join {{ref('employment_hero_organisations')}} eho on x.org_id = eho.id
    where eho.pricing_tier != 'free' and eho.pricing_tier is not null and eho.is_paying_eh
    group by 1,2,3,4,5
)

-- , feedback as (
--     select org_uuid, org_id, user_uuid, matched_job_title
--     , listagg(distinct vote, '; ') WITHIN GROUP (order by created_at desc) as votes
--     , listagg(distinct explanation, '; ') WITHIN GROUP (order by created_at desc) as explanations
--     from (        
--         select fb.id, fb.created_at, explanation, vote    
--         , u.id as feedback_user_id
--         , us.uuid as user_uuid
--         , o.uuid as org_uuid
--         , o.id as org_id
--         , json_extract_path_text(context, 'matching_job_title')::varchar as matched_job_title
--         from {{source('ats_public', 'feedbacks')}} fb
--         join {{source('postgres_public', 'users')}} u on fb.user_id = u.uuid
--         join {{ref('employment_hero_employees')}} e on e.user_id = u.id
--         join {{ref('employment_hero_organisations')}} o on e.organisation_id = o.id 
--         join {{source('postgres_public', 'users')}} us on json_extract_path_text(context, 'user_ids') ilike '%"' || us.uuid || '"%'
--         where not fb._fivetran_deleted
--         --and explanation is not null
--         and feature = 'candidate_recommendation'
--         and is_paying_eh
--     )
--     group by 1,2,3,4
--   )

, user_emails as (
    select u.id as user_id, u.uuid as user_uuid, u.email
    from {{source('postgres_public', 'users')}}  u 
    join {{ref('employment_hero_employees')}} e on e.user_id = u.id

    union

    select distinct u.id as user_id, u.uuid as user_uuid, e.personal_email as email
    from {{source('postgres_public', 'users')}}  u 
    join {{ref('employment_hero_employees')}} e on e.user_id = u.id
    where e.personal_email is not null    
)

, hired_from_saved_hired_at as (
    select org_uuid, org_id, user_uuid, employer_member_uuid, candidate_email
    , min(hired_at) as hired_at
    from (        
        Select j.organisation_id as org_uuid
        , o.id as org_id
        , cj.applied_email as candidate_email
        , cj.user_id as user_uuid  
        , ss.employer_member_uuid
        , cj.hired_at as hired_at
        From {{source('ats_public', 'candidate_jobs')}} cj
        join  {{source('ats_public', 'jobs')}} j on cj.job_id = j.id
        join {{ref('employment_hero_organisations')}} o on j.organisation_id = o.uuid
        join saved_or_shortlisted_candidates ss on ss.org_uuid = j.organisation_id and ss.user_uuid = cj.user_id and (ss.saved_or_shortlisted_at < cj.hired_at or cj.hired_at is null)
        where --not cj._fivetran_deleted
        --and not j._fivetran_deleted
        --and cj.result = 2 --'hired' --hired_at is not null
        cj.user_id is not null       
        and is_paying_eh

        union 

        Select j.organisation_id as org_uuid
        , o.id as org_id
        , cj.applied_email as candidate_email    
        , cj.user_id as user_uuid
        , ss.employer_member_uuid
        , cj.hired_at as hired_at
        From {{source('ats_public', 'candidate_jobs')}} cj
        join  {{source('ats_public', 'jobs')}} j on cj.job_id = j.id
        join {{ref('employment_hero_organisations')}} o on j.organisation_id = o.uuid
        join user_emails ue on ue.email = cj.applied_email
        join saved_or_shortlisted_candidates ss on ss.org_uuid = o.uuid and ss.user_uuid = ue.user_uuid and (ss.saved_or_shortlisted_at < cj.hired_at or cj.hired_at is null)
        
        where --not cj._fivetran_deleted
        --and not j._fivetran_deleted
        --and cj.result = 2 --'hired' --hired_at is not null
        cj.user_id is not null     
        and is_paying_eh
    )
    where hired_at is not null
    group by 1,2,3,4,5
)

,
onboard_from_saved as (
    select org_uuid, org_id, user_uuid, employer_member_uuid
    , listagg(distinct onboard_email, ', ') as onboard_email
    , listagg(distinct onboard_job_title, ', ') as onboard_job_title
    , min(onboarded_at) as onboarded_at
    from (
        Select o.uuid as org_uuid
        , o.id as org_id
        , e.email as onboard_email
        , e.onboard_job_title
        , e.user_uuid as user_uuid
        , ss.employer_member_uuid
        , e.created_at as onboarded_at
        From (
            select u.id as user_id, u.uuid as user_uuid, u.email, e.organisation_id, e.created_at, eh.title as onboard_job_title
            from {{source('postgres_public', 'users')}}  u 
            join {{ref('employment_hero_employees')}} e on e.user_id = u.id
            join {{first_row('postgres_public', 'employment_histories', 'member_id', 'created_at')}} as eh on eh.member_id = e.id  
            and (e.created_at < e.termination_date or e.termination_date is null)
        )e

         -- on cj.job_id = j.id
        join {{ref('employment_hero_organisations')}} o on e.organisation_id = o.id
        join saved_or_shortlisted_candidates ss on ss.org_uuid = o.uuid and ss.user_uuid = e.user_uuid and (ss.saved_or_shortlisted_at < e.created_at or e.created_at is null)
        where --not cj._fivetran_deleted
        --and not j._fivetran_deleted
        --and cj.result = 2 --'hired' --hired_at is not null
        e.user_uuid is not null       
        and is_paying_eh

        union 

        Select o.uuid as org_uuid
        , o.id as org_id
        , e.email as onboard_email
        , e.onboard_job_title     
        , e.user_uuid as user_uuid
        , ss.employer_member_uuid
        , e.created_at as onboarded_at
        From (
            select u.id as user_id, u.uuid as user_uuid, u.email, e.organisation_id, e.created_at, eh.title as onboard_job_title
            from {{source('postgres_public', 'users')}}  u 
            join {{ref('employment_hero_employees')}} e on e.user_id = u.id 
            join {{first_row('postgres_public', 'employment_histories', 'member_id', 'created_at')}} as eh on eh.member_id = e.id    
            and (e.created_at < e.termination_date or e.termination_date is null)
        )e
        join {{ref('employment_hero_organisations')}} o on e.organisation_id = o.id
        join user_emails ue on ue.email = e.email
        join saved_or_shortlisted_candidates ss on ss.org_uuid = o.uuid and ss.user_uuid = ue.user_uuid and (ss.saved_or_shortlisted_at < e.created_at or e.created_at is null)
        

        where --not cj._fivetran_deleted
        --and not j._fivetran_deleted
        --and cj.result = 2 --'hired' --hired_at is not null
        e.user_uuid is not null     
        and is_paying_eh
    )
    where onboarded_at is not null
    group by 1,2,3,4
)
, owners as (
  select distinct o.country, m.user_uuid, m.uuid as member_uuid, o.uuid as org_uuid
  from {{ref('employment_hero_employees')}} m
  join {{ref('employment_hero_organisations')}} o on m.organisation_id = o.id 
  where m.role ilike 'owner'
  and m.user_id is not null
  and m.uuid is not null
  )
  ,
  admins as (
  select distinct o.country, m.user_uuid, m.uuid as member_uuid, o.uuid as org_uuid
  from {{ref('employment_hero_employees')}} m
  join {{ref('employment_hero_organisations')}} o on m.organisation_id = o.id 
  where m.role ilike 'employer'
  and m.user_id is not null
  and m.uuid is not null
  )
  ,
  hiring_managers as (
  select distinct o.country, m.user_uuid, m.uuid as member_uuid, o.uuid as org_uuid
  from {{source('ats_public', 'hiring_managers')}} hm
  join {{ref('employment_hero_employees')}} m on hm.member_id = m.uuid
  join {{ref('employment_hero_organisations')}} o on m.organisation_id = o.id
  join {{ref('ats_jobs_created')}} j on hm.job_id = j.job_id
  where m.role not ilike 'owner'
  and m.role not ilike 'employer'
  and hm._fivetran_deleted ='f'
  and m.user_id is not null
  and m.uuid is not null
  )
  ,
  people_managers as (
  select distinct o.country, m.user_uuid, m.uuid as member_uuid, o.uuid as org_uuid
  from {{source('postgres_public', 'member_managers')}} pm
  join {{ref('employment_hero_employees')}} m on pm.manager_id = m.id
  join {{ref('employment_hero_organisations')}} o on m.organisation_id = o.id
  where m.role not ilike 'owner'
  and m.role not ilike 'employer'
  and m.user_id is not null
  and not pm._fivetran_deleted
  and m.uuid is not null
  )
  ,
  security_groups as (
  select rp.key, coalesce(ra.member_id, tm.member_id) as member_id, r.is_affecting_all_employees, r.organisation_id
  from {{source('postgres_public', 'security_roles')}} r
  join {{source('postgres_public', 'security_role_assignees')}} ra on ra.security_role_id = r.id
  join {{source('postgres_public', 'security_role_permissions')}} rp on rp.security_role_id = r.id
  join {{source('postgres_public', 'team_members')}} tm on tm.team_id = ra.team_id
  where rp.key = 'security_permissions_recruitment_ats' and rp.use = 't'
  and not r._fivetran_deleted
  and not ra._fivetran_deleted
  and not rp._fivetran_deleted
  and not tm._fivetran_deleted
  )
  , 

  tams as ( 
  select distinct o.country, m.user_uuid, m.uuid as member_uuid, o.uuid as org_uuid
  from security_groups sg
  join {{ref('employment_hero_employees')}} m on sg.member_id = m.id
  join {{ref('employment_hero_organisations')}} o on m.organisation_id = o.id
  where m.role not ilike 'owner'
  and m.role not ilike 'employer'
  and m.user_id is not null
  and m.uuid is not null
  )


-- select {{ dbt_utils.generate_surrogate_key(['u.id', 'e.organisation_id', 'e.created_at', 'eh.title']) }} as id, u.id as user_id, u.uuid as user_uuid, u.email, e.organisation_id, e.created_at, eh.title as onboard_job_title
--             from {{source('postgres_public', 'users')}}  u 
--             join {{ref('employment_hero_employees')}} e on e.user_id = u.id
--             join ehistory as eh on eh.member_id = e.id  
--             and (e.created_at < e.termination_date or e.termination_date is null)
-- where u.uuid = 'b0d3406e-48e4-461e-afa0-8724c1078c00'

-- select {{ dbt_utils.generate_surrogate_key(['b.user_uuid', 'b.org_uuid', 'b.job_matched']) }} as id
-- , case when owners.user_uuid is not null then 'Owner'
-- when admins.user_uuid is not null then 'Admin'
-- when tams.user_uuid is not null then 'TAM'
-- when hiring_managers.user_uuid is not null then 'Hiring Manager'
-- when people_managers.user_uuid is not null then 'People Manager'
-- else 'Other' end as User_Type,
-- b.*, u.email
-- , up.experience_job_titles
-- , fsha.hired_at
-- , os.onboarded_at
-- , os.onboard_job_title
-- , os.onboard_email
-- from base b 
-- left join owners on b.user_uuid = owners.user_uuid and b.org_uuid = owners.org_uuid
-- left join admins on b.user_uuid = admins.user_uuid and b.org_uuid = admins.org_uuid
-- left join tams on b.user_uuid = tams.user_uuid and b.org_uuid = tams.org_uuid
-- left join hiring_managers on b.user_uuid = hiring_managers.user_uuid and b.org_uuid = hiring_managers.org_uuid
-- left join people_managers on b.user_uuid = people_managers.user_uuid and b.org_uuid = people_managers.org_uuid
-- join {{source('postgres_public', 'users')}} u on b.user_uuid = u.uuid
-- left join {{source('ats_public', 'matching_profiles')}} up on up.user_id = b.user_uuid
-- left join hired_from_saved_hired_at fsha on fsha.user_uuid = b.user_uuid and fsha.org_uuid = b.org_uuid
-- left join onboard_from_saved os on os.user_uuid = b.user_uuid and os.org_uuid = b.org_uuid
-- where b.user_uuid = 'b0d3406e-48e4-461e-afa0-8724c1078c00'

, intermediate1 as
(
select distinct {{ dbt_utils.generate_surrogate_key(['b.user_uuid', 'b.org_uuid', 'b.job_matched', 'b.employer_member_uuid']) }} as id
, case when owners.member_uuid is not null then 'Owner'
when admins.member_uuid is not null then 'Admin'
when tams.member_uuid is not null then 'TAM'
when hiring_managers.member_uuid is not null then 'Hiring Manager'
when people_managers.member_uuid is not null then 'People Manager'
else 'Other' end as User_Type
, b.user_uuid
, b.employer_member_uuid
, b.org_uuid
, u.email as candidate_email
, os.onboard_email
, b.org_id
, b.job_matched
, up.experience_job_titles
--, up.applied_job_titles 
--, up.preference_job_titles
, b.first_shown_at
, b.No_Of_Shown_Actions
, b.first_previewed_at
, b.No_Of_Previewed_Actions
, b.saved_or_shortlisted_at
, b.saved_at
, b.No_Of_Saved_Actions
, b.shortlisted_at
, b.No_Of_Shortlisted_Actions
--, b.save_member_id
--, b.shortlist_member_id
, fsha.hired_at
, os.onboarded_at
, case when os.onboarded_at is not null or fsha.hired_at is not null then 1 else 0 end as No_Of_Onboarded_Actions
-- , row_number() over (partition by b.user_uuid, b.org_uuid, b.employer_member_uuid
--     order by coalesce(b.first_shown_at, b.first_previewed_at, b.saved_at, b.shortlisted_at, fsha.hired_at, os.onboarded_at)) as rn
-- , case when rn = 1 and coalesce(fsha.hired_at, os.onboarded_at) is not null then 1 else 0 end as No_Of_Onboarded_Actions
, os.onboard_job_title
, fb.votes as feedback_vote
, fb.explanations as feedback_explanation
from base b
left join owners on b.employer_member_uuid = owners.member_uuid and b.org_uuid = owners.org_uuid
left join admins on b.employer_member_uuid = admins.member_uuid and b.org_uuid = admins.org_uuid
left join tams on b.employer_member_uuid = tams.member_uuid and b.org_uuid = tams.org_uuid
left join hiring_managers on b.employer_member_uuid = hiring_managers.member_uuid and b.org_uuid = hiring_managers.org_uuid
left join people_managers on b.employer_member_uuid = people_managers.member_uuid and b.org_uuid = people_managers.org_uuid
join {{source('postgres_public', 'users')}} u on b.user_uuid = u.uuid
left join {{source('ats_public', 'matching_profiles')}} up on up.user_id = b.user_uuid
left join hired_from_saved_hired_at fsha on fsha.user_uuid = b.user_uuid and fsha.org_uuid = b.org_uuid and (fsha.employer_member_uuid = b.employer_member_uuid or fsha.employer_member_uuid is null or b.employer_member_uuid is null)
left join onboard_from_saved os on os.user_uuid = b.user_uuid and os.org_uuid = b.org_uuid and (os.employer_member_uuid = b.employer_member_uuid or os.employer_member_uuid is null or b.employer_member_uuid is null)
left join {{ref('ats_feedback')}} fb on fb.user_uuid = b.user_uuid and fb.org_uuid = b.org_uuid and (fb.matched_job_title = '' or fb.matched_job_title = b.job_matched)
)

select
-- *
id
, User_Type
, user_uuid
, employer_member_uuid
, org_uuid
, candidate_email
, onboard_email
, org_id
, job_matched
, experience_job_titles
, first_shown_at
, No_Of_Shown_Actions
, first_previewed_at
, No_Of_Previewed_Actions
, saved_or_shortlisted_at
, saved_at
, No_Of_Saved_Actions
, shortlisted_at
, No_Of_Shortlisted_Actions
, hired_at
, onboarded_at
-- , No_Of_Onboarded_Actions2
-- , case when onboarded_at is not null or hired_at is not null then 1 else 0 end as No_Of_Onboarded_Actions
, row_number() over (partition by user_uuid, org_uuid
    order by coalesce(hired_at, onboarded_at) nulls last) as rn
, case when rn = 1 and coalesce(hired_at, onboarded_at) is not null then 1 else 0 end as No_Of_Onboarded_Actions
, onboard_job_title
, feedback_vote
, feedback_explanation
from intermediate1







