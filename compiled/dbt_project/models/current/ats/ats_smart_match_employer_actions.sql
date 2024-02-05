

with candidates_shown as (
    select
        org_uuid,
        o.id        as org_id,
        s.user_uuid,
        s.employer_member_uuid,
        job_matched,
        min(s.time) as first_shown_at,
        count(*)    as no_of_shown_actions
    from "dev"."ats"."smart_match_candidates_shown" as s
    inner join "dev"."employment_hero"."organisations" as o on s.org_uuid = o.uuid and o.is_paying_eh
    group by 1, 2, 3, 4, 5
),

candidates_previewed as (
    select
        org_uuid,
        o.id        as org_id,
        s.user_uuid,
        s.employer_member_uuid,
        job_matched,
        min(s.time) as first_previewed_at,
        count(*)    as no_of_previewed_actions
    from "dev"."ats"."smart_match_candidates_previewed" as s
    inner join "dev"."employment_hero"."organisations" as o on s.org_uuid = o.uuid and o.is_paying_eh
    group by 1, 2, 3, 4, 5
),

shortlist_candidates_action as (
    select
        org_uuid,
        o.id        as org_id,
        s.user_uuid,
        s.employer_member_uuid,
        job_matched,
        min(s.time) as shortlisted_at,
        count(*)    as no_of_shortlisted_actions
    from "dev"."ats"."smart_match_candidates_shortlisted_actioned" as s
    inner join "dev"."employment_hero"."organisations" as o on s.org_uuid = o.uuid and o.is_paying_eh
    group by 1, 2, 3, 4, 5
),

saved_candidates_action as (
    select
        org_uuid,
        o.id        as org_id,
        s.user_uuid,
        s.employer_member_uuid,
        job_matched,
        min(s.time) as saved_at,
        count(*)    as no_of_saved_actions
    from "dev"."ats"."smart_match_candidates_saved_actioned" as s
    inner join "dev"."employment_hero"."organisations" as o on s.org_uuid = o.uuid and o.is_paying_eh
    group by 1, 2, 3, 4, 5
),

saved_or_shortlisted_candidates as (
    select
        coalesce(sc.org_uuid, sl.org_uuid)                                                                 as org_uuid,
        coalesce(sc.org_id, sl.org_id)                                                                     as org_id,
        coalesce(sc.user_uuid, sl.user_uuid)                                                               as user_uuid,
        coalesce(sc.employer_member_uuid, sl.employer_member_uuid)                                         as employer_member_uuid,
        coalesce(case when sl.job_matched = '' then NULL else sl.job_matched end, sc.job_matched)::varchar as job_matched,
        min(sc.saved_at)                                                                                   as saved_at,
        min(coalesce(sc.saved_at, sl.shortlisted_at))                                                      as saved_or_shortlisted_at,
        max(sc.no_of_saved_actions)                                                                        as no_of_saved_actions,
        min(sl.shortlisted_at)                                                                             as shortlisted_at,
        max(sl.no_of_shortlisted_actions)                                                                  as no_of_shortlisted_actions
    from saved_candidates_action as sc
    full outer join shortlist_candidates_action as sl
        on
            sc.org_uuid = sl.org_uuid and sc.user_uuid = sl.user_uuid
            and (sc.job_matched = sl.job_matched or sl.job_matched = '' or sc.job_matched = '')
            and (sc.employer_member_uuid = sl.employer_member_uuid or sl.employer_member_uuid is NULL or sc.employer_member_uuid is NULL)
            and date_trunc('day', sc.saved_at) <= date_trunc('day', sl.shortlisted_at)
    group by 1, 2, 3, 4, 5
),

previewed_saved_shortlisted as (
    select
        coalesce(s.org_uuid, p.org_uuid)                                                                as org_uuid,
        coalesce(s.org_id, p.org_id)                                                                    as org_id,
        coalesce(s.user_uuid, p.user_uuid)                                                              as user_uuid,
        coalesce(s.employer_member_uuid, p.employer_member_uuid)                                        as employer_member_uuid,
        coalesce(case when s.job_matched = '' then NULL else s.job_matched end, p.job_matched)::varchar as job_matched,
        min(p.first_previewed_at)                                                                       as first_previewed_at,
        sum(p.no_of_previewed_actions)                                                                  as no_of_previewed_actions,
        min(s.saved_at)                                                                                 as saved_at,
        sum(s.no_of_saved_actions)                                                                      as no_of_saved_actions,
        min(s.saved_or_shortlisted_at)                                                                  as saved_or_shortlisted_at,
        min(s.shortlisted_at)                                                                           as shortlisted_at,
        sum(s.no_of_shortlisted_actions)                                                                as no_of_shortlisted_actions

    from saved_or_shortlisted_candidates as s
    full outer join candidates_previewed as p
        on
            s.org_uuid = p.org_uuid and s.user_uuid = p.user_uuid
            and (s.job_matched = p.job_matched or s.job_matched = '' or p.job_matched = '')
            and (s.employer_member_uuid = p.employer_member_uuid or s.employer_member_uuid is NULL or p.employer_member_uuid is NULL)
            and date_trunc('day', p.first_previewed_at) <= date_trunc('day', s.saved_or_shortlisted_at)
    group by 1, 2, 3, 4, 5
),

shown_previewed_saved_shortlisted as (
    select
        coalesce(p.org_uuid, cs.org_uuid)                                                                as org_uuid,
        coalesce(p.org_id, cs.org_id)                                                                    as org_id,
        coalesce(p.user_uuid, cs.user_uuid)                                                              as user_uuid,
        coalesce(p.employer_member_uuid, cs.employer_member_uuid)                                        as employer_member_uuid,
        coalesce(case when p.job_matched = '' then NULL else p.job_matched end, cs.job_matched)::varchar as job_matched,
        min(cs.first_shown_at)                                                                           as first_shown_at,
        sum(cs.no_of_shown_actions)                                                                      as no_of_shown_actions,
        min(p.first_previewed_at)                                                                        as first_previewed_at,
        sum(p.no_of_previewed_actions)                                                                   as no_of_previewed_actions,
        min(p.saved_at)                                                                                  as saved_at,
        sum(p.no_of_saved_actions)                                                                       as no_of_saved_actions,
        min(p.saved_or_shortlisted_at)                                                                   as saved_or_shortlisted_at,
        min(p.shortlisted_at)                                                                            as shortlisted_at,
        sum(p.no_of_shortlisted_actions)                                                                 as no_of_shortlisted_actions
    from previewed_saved_shortlisted as p
    full outer join candidates_shown as cs
        on
            p.org_uuid = cs.org_uuid and p.user_uuid = cs.user_uuid
            and (p.job_matched = cs.job_matched or p.job_matched = '' or cs.job_matched = '')
            and (p.employer_member_uuid = cs.employer_member_uuid or p.employer_member_uuid is NULL or cs.employer_member_uuid is NULL)
            and date_trunc('day', cs.first_shown_at) <= date_trunc('day', p.first_previewed_at)
    group by 1, 2, 3, 4, 5
),

base as (
    select
        x.org_uuid,
        x.org_id,
        x.user_uuid,
        x.employer_member_uuid,
        x.job_matched,
        min(first_shown_at)            as first_shown_at,
        sum(no_of_shown_actions)       as no_of_shown_actions,
        min(first_previewed_at)        as first_previewed_at,
        sum(no_of_previewed_actions)   as no_of_previewed_actions,
        min(saved_at)                  as saved_at,
        sum(no_of_saved_actions)       as no_of_saved_actions,
        min(saved_or_shortlisted_at)   as saved_or_shortlisted_at,
        min(shortlisted_at)            as shortlisted_at,
        sum(no_of_shortlisted_actions) as no_of_shortlisted_actions
    from shown_previewed_saved_shortlisted as x
    left join "dev"."employment_hero"."organisations" as eho on x.org_id = eho.id
    where eho.pricing_tier != 'free' and eho.pricing_tier is not NULL and eho.is_paying_eh
    group by 1, 2, 3, 4, 5
),

user_emails as (
    select
        u.id   as user_id,
        u.uuid as user_uuid,
        u.email
    from "dev"."postgres_public"."users" as u
    inner join "dev"."employment_hero"."employees" as e on u.id = e.user_id

    union distinct

    select distinct
        u.id             as user_id,
        u.uuid           as user_uuid,
        e.personal_email as email
    from "dev"."postgres_public"."users" as u
    inner join "dev"."employment_hero"."employees" as e on u.id = e.user_id
    where e.personal_email is not NULL
),

hired_from_saved_hired_at as (
    select
        org_uuid,
        org_id,
        user_uuid,
        employer_member_uuid,
        candidate_email,
        min(hired_at) as hired_at
    from (
        select
            j.organisation_id as org_uuid,
            o.id              as org_id,
            cj.applied_email  as candidate_email,
            cj.user_id        as user_uuid,
            ss.employer_member_uuid,
            cj.hired_at       as hired_at
        from "dev"."ats_public"."candidate_jobs" as cj
        inner join "dev"."ats_public"."jobs" as j on cj.job_id = j.id
        inner join "dev"."employment_hero"."organisations" as o on j.organisation_id = o.uuid
        inner join saved_or_shortlisted_candidates as ss on j.organisation_id = ss.org_uuid and cj.user_id = ss.user_uuid and (cj.hired_at > ss.saved_or_shortlisted_at or cj.hired_at is NULL)
        where
            cj.user_id is not NULL
            and is_paying_eh

        union distinct

        select
            j.organisation_id as org_uuid,
            o.id              as org_id,
            cj.applied_email  as candidate_email,
            cj.user_id        as user_uuid,
            ss.employer_member_uuid,
            cj.hired_at       as hired_at
        from "dev"."ats_public"."candidate_jobs" as cj
        inner join "dev"."ats_public"."jobs" as j on cj.job_id = j.id
        inner join "dev"."employment_hero"."organisations" as o on j.organisation_id = o.uuid
        inner join user_emails as ue on cj.applied_email = ue.email
        inner join saved_or_shortlisted_candidates as ss on o.uuid = ss.org_uuid and ss.user_uuid = ue.user_uuid and (cj.hired_at > ss.saved_or_shortlisted_at or cj.hired_at is NULL)

        where
            cj.user_id is not NULL
            and is_paying_eh
    )
    where hired_at is not NULL
    group by 1, 2, 3, 4, 5
)

, user_employment_details as (
    select
        u.id     as user_id,
        u.uuid   as user_uuid,
        u.email,
        e.organisation_id,
        e.created_at,
        eh.title as onboard_job_title
    from "dev"."postgres_public"."users" as u
    inner join "dev"."employment_hero"."employees" as e on u.id = e.user_id
    inner join 

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

 as eh on e.id = eh.member_id
        and (e.created_at < e.termination_date or e.termination_date is NULL)
),

onboarded_int as (
    select
        o.uuid       as org_uuid,
        o.id         as org_id,
        e.email      as onboard_email,
        e.onboard_job_title,
        e.user_uuid  as user_uuid,
        ss.employer_member_uuid,
        e.created_at as onboarded_at
    from user_employment_details as e
    inner join "dev"."employment_hero"."organisations" as o on e.organisation_id = o.id
    inner join saved_or_shortlisted_candidates as ss on o.uuid = ss.org_uuid and e.user_uuid = ss.user_uuid and (e.created_at > ss.saved_or_shortlisted_at or e.created_at is NULL)
    where
        e.user_uuid is not NULL
        and is_paying_eh

    union distinct

    select
        o.uuid       as org_uuid,
        o.id         as org_id,
        e.email      as onboard_email,
        e.onboard_job_title,
        e.user_uuid  as user_uuid,
        ss.employer_member_uuid,
        e.created_at as onboarded_at
    from user_employment_details as e
    inner join "dev"."employment_hero"."organisations" as o on e.organisation_id = o.id
    inner join user_emails as ue on e.email = ue.email
    inner join saved_or_shortlisted_candidates as ss on o.uuid = ss.org_uuid and ue.user_uuid = ss.user_uuid and (e.created_at > ss.saved_or_shortlisted_at or e.created_at is NULL)
    where e.user_uuid is not NULL and is_paying_eh
)
,
onboard_from_saved as (
    select
        org_uuid,
        org_id,
        user_uuid,
        employer_member_uuid,
        listagg(distinct onboard_email, ', ')     as onboard_email,
        listagg(distinct onboard_job_title, ', ') as onboard_job_title,
        min(onboarded_at)                         as onboarded_at
    from onboarded_int
    where onboarded_at is not NULL
    group by 1, 2, 3, 4
),

owners as (
    select distinct
        o.country,
        m.user_uuid,
        m.uuid as member_uuid,
        o.uuid as org_uuid
    from "dev"."employment_hero"."employees" as m
    inner join "dev"."employment_hero"."organisations" as o on m.organisation_id = o.id
    where
        m.role ilike 'owner'
        and m.user_id is not NULL
        and m.uuid is not NULL
)
,
admins as (
    select distinct
        o.country,
        m.user_uuid,
        m.uuid as member_uuid,
        o.uuid as org_uuid
    from "dev"."employment_hero"."employees" as m
    inner join "dev"."employment_hero"."organisations" as o on m.organisation_id = o.id
    where
        m.role ilike 'employer'
        and m.user_id is not NULL
        and m.uuid is not NULL
)
,
hiring_managers as (
    select distinct
        o.country,
        m.user_uuid,
        m.uuid as member_uuid,
        o.uuid as org_uuid
    from "dev"."ats_public"."hiring_managers" as hm
    inner join "dev"."employment_hero"."employees" as m on hm.member_id = m.uuid
    inner join "dev"."employment_hero"."organisations" as o on m.organisation_id = o.id
    inner join "dev"."ats"."jobs_created" as j on hm.job_id = j.job_id
    where
        m.role not ilike 'owner'
        and m.role not ilike 'employer'
        and hm._fivetran_deleted = 'f'
        and m.user_id is not NULL
        and m.uuid is not NULL
)
,
people_managers as (
    select distinct
        o.country,
        m.user_uuid,
        m.uuid as member_uuid,
        o.uuid as org_uuid
    from "dev"."postgres_public"."member_managers" as pm
    inner join "dev"."employment_hero"."employees" as m on pm.manager_id = m.id
    inner join "dev"."employment_hero"."organisations" as o on m.organisation_id = o.id
    where
        m.role not ilike 'owner'
        and m.role not ilike 'employer'
        and m.user_id is not NULL
        and not pm._fivetran_deleted
        and m.uuid is not NULL
)
,
security_groups as (
    select
        rp.key,
        coalesce(ra.member_id, tm.member_id) as member_id,
        r.is_affecting_all_employees,
        r.organisation_id
    from "dev"."postgres_public"."security_roles" as r
    inner join "dev"."postgres_public"."security_role_assignees" as ra on r.id = ra.security_role_id
    inner join "dev"."postgres_public"."security_role_permissions" as rp on r.id = rp.security_role_id
    inner join "dev"."postgres_public"."team_members" as tm on ra.team_id = tm.team_id
    where
        rp.key = 'security_permissions_recruitment_ats' and rp.use = 't'
        and not r._fivetran_deleted
        and not ra._fivetran_deleted
        and not rp._fivetran_deleted
        and not tm._fivetran_deleted
)
,

tams as (
    select distinct
        o.country,
        m.user_uuid,
        m.uuid as member_uuid,
        o.uuid as org_uuid
    from security_groups as sg
    inner join "dev"."employment_hero"."employees" as m on sg.member_id = m.id
    inner join "dev"."employment_hero"."organisations" as o on m.organisation_id = o.id
    where
        m.role not ilike 'owner'
        and m.role not ilike 'employer'
        and m.user_id is not NULL
        and m.uuid is not NULL
),

intermediate1 as (
    select distinct
        md5(cast(coalesce(cast(b.user_uuid as TEXT), '_dbt_utils_surrogate_key_null_') || '-' || coalesce(cast(b.org_uuid as TEXT), '_dbt_utils_surrogate_key_null_') || '-' || coalesce(cast(b.job_matched as TEXT), '_dbt_utils_surrogate_key_null_') || '-' || coalesce(cast(b.employer_member_uuid as TEXT), '_dbt_utils_surrogate_key_null_') as TEXT)) as id,
        case
            when owners.member_uuid is not NULL then 'Owner'
            when admins.member_uuid is not NULL then 'Admin'
            when tams.member_uuid is not NULL then 'TAM'
            when hiring_managers.member_uuid is not NULL then 'Hiring Manager'
            when people_managers.member_uuid is not NULL then 'People Manager'
            else 'Other'
        end                                                                                                                                                                                                                                                                                                                                                  as user_type,
        b.user_uuid,
        b.employer_member_uuid,
        b.org_uuid,
        u.email                                                                                                                                                                                                                                                                                                                                              as candidate_email,
        os.onboard_email,
        b.org_id,
        b.job_matched,
        up.experience_job_titles,
        b.first_shown_at,
        b.no_of_shown_actions,
        b.first_previewed_at,
        b.no_of_previewed_actions,
        b.saved_or_shortlisted_at,
        b.saved_at,
        b.no_of_saved_actions,
        b.shortlisted_at,
        b.no_of_shortlisted_actions,
        fsha.hired_at,
        os.onboarded_at,
        case when os.onboarded_at is not NULL or fsha.hired_at is not NULL then 1 else 0 end                                                                                                                                                                                                                                                                 as no_of_onboarded_actions,
        os.onboard_job_title,
        fb.votes                                                                                                                                                                                                                                                                                                                                             as feedback_vote,
        fb.explanations                                                                                                                                                                                                                                                                                                                                      as feedback_explanation
    from base as b
    left join owners on b.employer_member_uuid = owners.member_uuid and b.org_uuid = owners.org_uuid
    left join admins on b.employer_member_uuid = admins.member_uuid and b.org_uuid = admins.org_uuid
    left join tams on b.employer_member_uuid = tams.member_uuid and b.org_uuid = tams.org_uuid
    left join hiring_managers on b.employer_member_uuid = hiring_managers.member_uuid and b.org_uuid = hiring_managers.org_uuid
    left join people_managers on b.employer_member_uuid = people_managers.member_uuid and b.org_uuid = people_managers.org_uuid
    inner join "dev"."postgres_public"."users" as u on b.user_uuid = u.uuid
    left join "dev"."ats_public"."matching_profiles" as up on b.user_uuid = up.user_id
    left join hired_from_saved_hired_at as fsha on b.user_uuid = fsha.user_uuid and b.org_uuid = fsha.org_uuid and (b.employer_member_uuid = fsha.employer_member_uuid or fsha.employer_member_uuid is NULL or b.employer_member_uuid is NULL)
    left join onboard_from_saved as os on b.user_uuid = os.user_uuid and b.org_uuid = os.org_uuid and (b.employer_member_uuid = os.employer_member_uuid or os.employer_member_uuid is NULL or b.employer_member_uuid is NULL)
    left join "dev"."ats"."feedback" as fb on b.user_uuid = fb.user_uuid and b.org_uuid = fb.org_uuid and (fb.matched_job_title = '' or b.job_matched = fb.matched_job_title)
)

select
    id,
    user_type,
    user_uuid,
    employer_member_uuid,
    org_uuid,
    candidate_email,
    onboard_email,
    org_id,
    job_matched,
    experience_job_titles,
    first_shown_at,
    no_of_shown_actions,
    first_previewed_at,
    no_of_previewed_actions,
    saved_or_shortlisted_at,
    saved_at,
    no_of_saved_actions,
    shortlisted_at,
    no_of_shortlisted_actions,
    hired_at,
    onboarded_at,
    row_number() over (
        partition by user_uuid, org_uuid
        order by coalesce(hired_at, onboarded_at) nulls last
    )                                                                                   as rn,
    case when rn = 1 and coalesce(hired_at, onboarded_at) is not NULL then 1 else 0 end as no_of_onboarded_actions,
    onboard_job_title,
    feedback_vote,
    feedback_explanation
from intermediate1