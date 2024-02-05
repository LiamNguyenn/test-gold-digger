

with candidates_shown as (
  Select org_uuid
  , o.id as org_id
  , s.user_uuid
  , case when raw_job_matched = '' then job_matched else raw_job_matched end as job_matched
  , s.time as first_shown_at
from 
    ( select *, row_number() OVER (PARTITION BY org_uuid, user_uuid, case when raw_job_matched = '' then job_matched else raw_job_matched end ORDER BY "time" ASC) as rn
        from  "dev"."ats"."smart_match_candidates_shown" 
    )s
  join "dev"."employment_hero"."organisations" o on s.org_uuid = o.uuid and o.is_paying_eh  
  where s.rn = 1
)

, candidates_previewed as (
  Select org_uuid
  , o.id as org_id
  , s.user_uuid
  , case when raw_job_matched = '' then job_matched else raw_job_matched end as job_matched
  --, e.id as first_previewed_member_id  
  , s.time as first_previewed_at
from ( 
    select *, row_number() OVER (PARTITION BY org_uuid, user_uuid, case when raw_job_matched = '' then job_matched else raw_job_matched end  ORDER BY "time" ASC) as rn
    from "dev"."ats"."smart_match_candidates_previewed" ) s
  join "dev"."employment_hero"."organisations" o on s.org_uuid = o.uuid and o.is_paying_eh
  left join "dev"."employment_hero"."employees" e on e.uuid = s.employer_member_uuid
    where s.rn = 1
)

, shortlist_candidates_action as (
  Select org_uuid
  , o.id as org_id
  , s.user_uuid
  , case when raw_job_matched = '' then job_matched else raw_job_matched end as job_matched
  --, e.id as shortlist_member_id
  , s.time as shortlisted_at
from (
    select *, row_number() OVER (PARTITION BY org_uuid, user_uuid, case when raw_job_matched = '' then job_matched else raw_job_matched end ORDER BY "time" ASC) as rn
    from "dev"."ats"."smart_match_candidates_shortlisted_actioned" 
    )s
  join "dev"."employment_hero"."organisations" o on s.org_uuid = o.uuid and o.is_paying_eh
  left join "dev"."employment_hero"."employees" e on e.uuid = s.employer_member_uuid
  where s.rn = 1
)

, shortlist_candidates_db_records as (
      Select distinct j.organisation_id as org_uuid
  , o.id as org_id
  , cj.user_id as user_uuid
  , coalesce(j.title, '')::varchar as job_matched
  , min(cj.created_at) as shortlisted_at
From "dev"."ats_public"."candidate_jobs" cj
join "dev"."ats_public"."jobs" as j on cj.job_id = j.id
join "dev"."employment_hero"."organisations" o on j.organisation_id = o.uuid
left Join "dev"."ats_public"."hiring_phases" hp on cj.hiring_phase_id = hp.id
Where cj.source_name = 'Shortlisted from Saved candidate'
and is_paying_eh
group by 1,2,3,4
)

, shortlisted_candidates as (
    select distinct coalesce(a.org_uuid, d.org_uuid) as org_uuid
    , coalesce(a.org_id, d.org_id) as org_id
    , coalesce(a.user_uuid, d.user_uuid) as user_uuid
    , coalesce(case when a.job_matched = '' then null else a.job_matched end, d.job_matched)::varchar as job_matched
    , coalesce(a.shortlisted_at, d.shortlisted_at) as shortlisted_at
    --, a.shortlist_member_id
    from shortlist_candidates_action a 
    full outer join shortlist_candidates_db_records d 
        on d.org_uuid = a.org_uuid and d.user_uuid  = a.user_uuid 
        and (a.job_matched = d.job_matched or a.job_matched = '' or d.job_matched = '') 
        and date_trunc('day', a.shortlisted_at) <= date_trunc('day', d.shortlisted_at)
)

, saved_candidates_action as (
  Select org_uuid
  , o.id as org_id
  , s.user_uuid
  , case when raw_job_matched = '' then job_matched else raw_job_matched end as job_matched
  --, e.id as save_member_id
  , s.time as saved_at
from (
    select *, row_number() OVER (PARTITION BY org_uuid, user_uuid, case when raw_job_matched = '' then job_matched else raw_job_matched end ORDER BY "time" ASC) as rn
    from "dev"."ats"."smart_match_candidates_saved_actioned" 
    )s
  join "dev"."employment_hero"."organisations" o on s.org_uuid = o.uuid and o.is_paying_eh
  left join "dev"."employment_hero"."employees" e on e.uuid = s.employer_member_uuid
  where s.rn = 1
)

, saved_candidates_db_records as (
Select  
  o.uuid as org_uuid
  , o.id as org_id
  , u.uuid as user_uuid
  , coalesce(sc.job_saved_for, '')::varchar as job_matched
  --, author_id as save_member_id
  , min(sc.created_at) as saved_at
  from "dev"."postgres_public"."saved_candidates" sc
  join "dev"."employment_hero"."organisations" o on o.id = sc.organisation_id
  join "dev"."postgres_public"."users" u on u.id = sc.user_id
  where is_paying_eh
    group by 1,2,3,4
)

, saved_candidates as (
    select distinct coalesce(a.org_uuid, d.org_uuid) as org_uuid
    , coalesce(a.org_id, d.org_id) as org_id
    , coalesce(a.user_uuid, d.user_uuid) as user_uuid
    , coalesce(case when a.job_matched = '' then null else a.job_matched end, d.job_matched)::varchar as job_matched
    , coalesce(a.saved_at, d.saved_at) as saved_at
    --, coalesce(a.save_member_id, d.save_member_id) as save_member_id
    from saved_candidates_action a 
    full outer join saved_candidates_db_records d
        on d.org_uuid = a.org_uuid and d.user_uuid  = a.user_uuid 
        and (a.job_matched = d.job_matched or a.job_matched = '' or d.job_matched = '') 
        and date_trunc('day', a.saved_at) <= date_trunc('day', d.saved_at)
)

, saved_or_shortlisted_candidates as (
    select distinct
    coalesce(sc.org_uuid, sl.org_uuid) as org_uuid
    , coalesce(sc.org_id, sl.org_id) as org_id
    , coalesce(sc.user_uuid, sl.user_uuid) as user_uuid
    , coalesce(case when sl.job_matched = '' then null else sl.job_matched end, sc.job_matched)::varchar as job_matched
    , coalesce(sc.saved_at, sl.shortlisted_at) as saved_or_shortlisted_at
    --, sc.save_member_id
    , sl.shortlisted_at as shortlisted_at
    --, sl.shortlist_member_id
    from saved_candidates sc 
    full outer join shortlisted_candidates sl 
        on sc.org_uuid = sl.org_uuid and sc.user_uuid = sl.user_uuid 
        and (sl.job_matched = sc.job_matched or sl.job_matched = '' or sc.job_matched = '')
        and date_trunc('day', sc.saved_at) <= date_trunc('day', sl.shortlisted_at)
)

, base as (
    select x.org_uuid
    , x.org_id
    , x.user_uuid
    , x.job_matched
    , eho.country
    , eho.industry
    , a.city as org_city
    , g.latitude as org_latitude
    , g.longitude as org_longitude
    --, save_member_id
    --, shortlist_member_id
    , min(first_shown_at) as first_shown_at
    , min(first_previewed_at) as first_previewed_at
    , min(saved_or_shortlisted_at) as saved_or_shortlisted_at
    , min(shortlisted_at) as shortlisted_at
    from (
        select coalesce(p.org_uuid, cs.org_uuid) as org_uuid
        , coalesce(p.org_id, cs.org_id) as org_id
        , coalesce(p.user_uuid, cs.user_uuid) as user_uuid
        , coalesce(case when p.job_matched = '' then null else p.job_matched end, cs.job_matched)::varchar as job_matched
        , cs.first_shown_at
        , p.first_previewed_at
        , p.saved_or_shortlisted_at
        , p.shortlisted_at 
        --, p.save_member_id
        --, p.shortlist_member_id
        from (
            select coalesce(s.org_uuid, p.org_uuid) as org_uuid
            , coalesce(s.org_id, p.org_id) as org_id
            , coalesce(s.user_uuid, p.user_uuid) as user_uuid
            , coalesce(case when s.job_matched = '' then null else s.job_matched end, p.job_matched)::varchar as job_matched
            , p.first_previewed_at
            , s.saved_or_shortlisted_at
            --, s.save_member_id            
            , s.shortlisted_at
            --, s.shortlist_member_id
            from saved_or_shortlisted_candidates s
            full outer join candidates_previewed p 
                on s.org_uuid = p.org_uuid and s.user_uuid = p.user_uuid 
                and (s.job_matched = p.job_matched or s.job_matched = '' or p.job_matched = '') 
                and date_trunc('day', p.first_previewed_at) <= date_trunc('day', s.saved_or_shortlisted_at)
            )p
            full outer join candidates_shown cs
                on p.org_uuid = cs.org_uuid and p.user_uuid = cs.user_uuid 
                and (p.job_matched = cs.job_matched or p.job_matched = '' or cs.job_matched = '') 
                and date_trunc('day', cs.first_shown_at) <= date_trunc('day', p.first_previewed_at)
    ) x
    join "dev"."employment_hero"."organisations" eho on x.org_id = eho.id
    left join "dev"."postgres_public"."addresses" a on eho.primary_address_id = a.id and not a._fivetran_deleted
    left join (
        select *
        from "dev"."postgres_public"."address_geolocations"
        where id in (
            select FIRST_VALUE(id) over(partition by address_id order by updated_at desc rows between unbounded preceding and unbounded following)
            from "dev"."postgres_public"."address_geolocations"
            where not _fivetran_deleted
            and latitude is not null 
            and longitude is not null
            )
        )g on g.address_id = a.id 
    where eho.pricing_tier != 'free' and eho.pricing_tier is not null
    group by 1,2,3,4,5,6,7,8,9
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
--         from "dev"."ats_public"."feedbacks" fb
--         join "dev"."postgres_public"."users" u on fb.user_id = u.uuid
--         join "dev"."employment_hero"."employees" e on e.user_id = u.id
--         join "dev"."employment_hero"."organisations" o on e.organisation_id = o.id 
--         join "dev"."postgres_public"."users" us on json_extract_path_text(context, 'user_ids') ilike '%"' || us.uuid || '"%'
--         where not fb._fivetran_deleted
--         --and explanation is not null
--         and feature = 'candidate_recommendation'
--         and is_paying_eh
--     )
--     group by 1,2,3,4
--   )

, user_emails as (
    select u.id as user_id, u.uuid as user_uuid, u.email
    from "dev"."postgres_public"."users"  u 
    join "dev"."employment_hero"."employees" e on e.user_id = u.id

    union

    select distinct u.id as user_id, u.uuid as user_uuid, e.personal_email as email
    from "dev"."postgres_public"."users"  u 
    join "dev"."employment_hero"."employees" e on e.user_id = u.id
    where e.personal_email is not null    
)

, hired_from_saved_hired_at as (
    -- removed emails as some candidates can use different emails for different applications even with the same user uuid
    select org_uuid, org_id, user_uuid
    , min(hired_at) as hired_at
    from (        
        Select j.organisation_id as org_uuid
        , o.id as org_id
        , cj.applied_email as candidate_email
        , cj.user_id as user_uuid  
        , cj.hired_at as hired_at
        From "dev"."ats_public"."candidate_jobs" cj
        join  "dev"."ats_public"."jobs" j on cj.job_id = j.id
        join "dev"."employment_hero"."organisations" o on j.organisation_id = o.uuid
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
        , cj.hired_at as hired_at
        From "dev"."ats_public"."candidate_jobs" cj
        join  "dev"."ats_public"."jobs" j on cj.job_id = j.id
        join "dev"."employment_hero"."organisations" o on j.organisation_id = o.uuid
        join user_emails ue on ue.email = cj.applied_email
        join saved_or_shortlisted_candidates ss on ss.org_uuid = o.uuid and ss.user_uuid = ue.user_uuid and (ss.saved_or_shortlisted_at < cj.hired_at or cj.hired_at is null)
        
        where --not cj._fivetran_deleted
        --and not j._fivetran_deleted
        --and cj.result = 2 --'hired' --hired_at is not null
        cj.user_id is not null     
        and is_paying_eh
    )
    group by 1,2,3
)

,
onboard_from_saved as (
    select org_uuid, org_id, user_uuid
    , listagg(distinct onboard_email, ', ') as onboard_email
    , listagg(distinct onboard_job_title, ', ') as onboard_job_title
    , min(onboarded_at) as onboarded_at
    from (
        Select o.uuid as org_uuid
        , o.id as org_id
        , e.email as onboard_email
        , e.onboard_job_title
        , e.user_uuid as user_uuid
        , e.created_at as onboarded_at
        From (
            select u.id as user_id, u.uuid as user_uuid, u.email, e.organisation_id, e.created_at, eh.title as onboard_job_title
            from "dev"."postgres_public"."users"  u 
            join "dev"."employment_hero"."employees" e on e.user_id = u.id
            join 

(
    select
        *
    from
        "dev"."postgres_public"."employment_histories"
    where
        id in (
        select
            FIRST_VALUE(id) over(partition by member_id order by created_at asc rows between unbounded preceding and unbounded following)
        from
            "dev"."postgres_public"."employment_histories"
        where
            not _fivetran_deleted
        )
)

 as eh on eh.member_id = e.id  
            and (e.created_at < e.termination_date or e.termination_date is null)
        )e

         -- on cj.job_id = j.id
        join "dev"."employment_hero"."organisations" o on e.organisation_id = o.id
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
        , e.created_at as onboarded_at
        From (
            select u.id as user_id, u.uuid as user_uuid, u.email, e.organisation_id, e.created_at, eh.title as onboard_job_title
            from "dev"."postgres_public"."users"  u 
            join "dev"."employment_hero"."employees" e on e.user_id = u.id 
            join 

(
    select
        *
    from
        "dev"."postgres_public"."employment_histories"
    where
        id in (
        select
            FIRST_VALUE(id) over(partition by member_id order by created_at asc rows between unbounded preceding and unbounded following)
        from
            "dev"."postgres_public"."employment_histories"
        where
            not _fivetran_deleted
        )
)

 as eh on eh.member_id = e.id    
            and (e.created_at < e.termination_date or e.termination_date is null)
        )e
        join "dev"."employment_hero"."organisations" o on e.organisation_id = o.id
        join user_emails ue on ue.email = e.email
        join saved_or_shortlisted_candidates ss on ss.org_uuid = o.uuid and ss.user_uuid = ue.user_uuid and (ss.saved_or_shortlisted_at < e.created_at or e.created_at is null)
        

        where --not cj._fivetran_deleted
        --and not j._fivetran_deleted
        --and cj.result = 2 --'hired' --hired_at is not null
        e.user_uuid is not null     
        and is_paying_eh
    )
    group by 1,2,3
)
-- , owners as (
--   select distinct o.country, m.user_uuid, o.uuid as org_uuid
--   from "dev"."employment_hero"."employees" m
--   join "dev"."employment_hero"."organisations" o on m.organisation_id = o.id
--   where m.role ilike 'owner'
--   and m.user_id is not null
--   )
--   ,
--   admins as (
--   select distinct o.country, m.user_uuid, o.uuid as org_uuid
--   from "dev"."employment_hero"."employees" m
--   join "dev"."employment_hero"."organisations" o on m.organisation_id = o.id 
--   where m.role ilike 'employer'
--   and m.user_id is not null
--   )
--   ,
--   hiring_managers as (
--   select distinct o.country, m.user_uuid, o.uuid as org_uuid
--   from "dev"."ats_public"."hiring_managers" hm
--   join "dev"."employment_hero"."employees" m on hm.member_id = m.uuid
--   join "dev"."employment_hero"."organisations" o on m.organisation_id = o.id
--   join "dev"."ats"."jobs_created" j on hm.job_id = j.job_id
--   where m.role not ilike 'owner'
--   and m.role not ilike 'employer'
--   and hm._fivetran_deleted ='f'
--   and m.user_id is not null
--   )
--   ,
--   people_managers as (
--   select distinct o.country, m.user_uuid, o.uuid as org_uuid
--   from "dev"."postgres_public"."member_managers" pm
--   join "dev"."employment_hero"."employees" m on pm.manager_id = m.id
--   join "dev"."employment_hero"."organisations" o on m.organisation_id = o.id
--   where m.role not ilike 'owner'
--   and m.role not ilike 'employer'
--   and m.user_id is not null
--   and not pm._fivetran_deleted
--   )
--   ,
--   security_groups as (
--   select rp.key, coalesce(ra.member_id, tm.member_id) as member_id, r.is_affecting_all_employees, r.organisation_id
--   from "dev"."postgres_public"."security_roles" r
--   join "dev"."postgres_public"."security_role_assignees" ra on ra.security_role_id = r.id
--   join "dev"."postgres_public"."security_role_permissions" rp on rp.security_role_id = r.id
--   join "dev"."postgres_public"."team_members" tm on tm.team_id = ra.team_id
--   where rp.key = 'security_permissions_recruitment_ats' and rp.use = 't'
--   and not r._fivetran_deleted
--   and not ra._fivetran_deleted
--   and not rp._fivetran_deleted
--   and not tm._fivetran_deleted
--   )
--   , 

--   tams as ( 
--   select distinct o.country, m.user_uuid, o.uuid as org_uuid
--   from security_groups sg
--   join "dev"."employment_hero"."employees" m on sg.member_id = m.id
--   join "dev"."employment_hero"."organisations" o on m.organisation_id = o.id
--   where m.role not ilike 'owner'
--   and m.role not ilike 'employer'
--   and m.user_id is not null
--   )


-- select md5(cast(coalesce(cast(u.id as TEXT), '_dbt_utils_surrogate_key_null_') || '-' || coalesce(cast(e.organisation_id as TEXT), '_dbt_utils_surrogate_key_null_') || '-' || coalesce(cast(e.created_at as TEXT), '_dbt_utils_surrogate_key_null_') || '-' || coalesce(cast(eh.title as TEXT), '_dbt_utils_surrogate_key_null_') as TEXT)) as id, u.id as user_id, u.uuid as user_uuid, u.email, e.organisation_id, e.created_at, eh.title as onboard_job_title
--             from "dev"."postgres_public"."users"  u 
--             join "dev"."employment_hero"."employees" e on e.user_id = u.id
--             join ehistory as eh on eh.member_id = e.id  
--             and (e.created_at < e.termination_date or e.termination_date is null)
-- where u.uuid = 'b0d3406e-48e4-461e-afa0-8724c1078c00'

-- select md5(cast(coalesce(cast(b.user_uuid as TEXT), '_dbt_utils_surrogate_key_null_') || '-' || coalesce(cast(b.org_uuid as TEXT), '_dbt_utils_surrogate_key_null_') || '-' || coalesce(cast(b.job_matched as TEXT), '_dbt_utils_surrogate_key_null_') as TEXT)) as id
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
-- join "dev"."postgres_public"."users" u on b.user_uuid = u.uuid
-- left join "dev"."ats_public"."matching_profiles" up on up.user_id = b.user_uuid
-- left join hired_from_saved_hired_at fsha on fsha.user_uuid = b.user_uuid and fsha.org_uuid = b.org_uuid
-- left join onboard_from_saved os on os.user_uuid = b.user_uuid and os.org_uuid = b.org_uuid
-- where b.user_uuid = 'b0d3406e-48e4-461e-afa0-8724c1078c00'

select md5(cast(coalesce(cast(b.user_uuid as TEXT), '_dbt_utils_surrogate_key_null_') || '-' || coalesce(cast(b.org_uuid as TEXT), '_dbt_utils_surrogate_key_null_') || '-' || coalesce(cast(b.job_matched as TEXT), '_dbt_utils_surrogate_key_null_') as TEXT)) as id
-- , case when owners.user_uuid is not null then 'Owner'
-- when admins.user_uuid is not null then 'Admin'
-- when tams.user_uuid is not null then 'TAM'
-- when hiring_managers.user_uuid is not null then 'Hiring Manager'
-- when people_managers.user_uuid is not null then 'People Manager'
-- else 'Other' end as User_Type
, b.user_uuid
, b.org_uuid
, b.country
, b.org_city
, b.org_latitude
, b.org_longitude
, b.industry
, u.email as candidate_email
, os.onboard_email
, b.org_id
, b.job_matched
, up.experience_job_titles
--, up.applied_job_titles 
--, up.preference_job_titles
, b.first_shown_at
, b.first_previewed_at
, b.saved_or_shortlisted_at
, b.shortlisted_at
--, b.save_member_id
--, b.shortlist_member_id
, coalesce(fsha.hired_at, os.onboarded_at) as hired_at
, os.onboarded_at
, os.onboard_job_title
, fb.votes as feedback_vote
, fb.explanations as feedback_explanation
from base b
-- left join owners on b.user_uuid = owners.user_uuid and b.org_uuid = owners.org_uuid
-- left join admins on b.user_uuid = admins.user_uuid and b.org_uuid = admins.org_uuid
-- left join tams on b.user_uuid = tams.user_uuid and b.org_uuid = tams.org_uuid
-- left join hiring_managers on b.user_uuid = hiring_managers.user_uuid and b.org_uuid = hiring_managers.org_uuid
-- left join people_managers on b.user_uuid = people_managers.user_uuid and b.org_uuid = people_managers.org_uuid
join "dev"."postgres_public"."users" u on b.user_uuid = u.uuid
left join "dev"."ats_public"."matching_profiles" up on up.user_id = b.user_uuid
left join hired_from_saved_hired_at fsha on fsha.user_uuid = b.user_uuid and fsha.org_uuid = b.org_uuid
left join onboard_from_saved os on os.user_uuid = b.user_uuid and os.org_uuid = b.org_uuid
left join "dev"."ats"."feedback" fb on fb.user_uuid = b.user_uuid and fb.org_uuid = b.org_uuid and (fb.matched_job_title = '' or fb.matched_job_title = b.job_matched)