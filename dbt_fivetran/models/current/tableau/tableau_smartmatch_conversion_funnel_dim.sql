{{ config(alias='smartmatch_conversion_funnel_dim') }}


with owners as (
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

  ,

  user_list as (
  select distinct employer_member_uuid, org_uuid from {{ref('ats_smart_match_employer_actions')}} where employer_member_uuid is not null and org_uuid is not null)

  select 
  a.employer_member_uuid
  , a.org_uuid
  , case when owners.member_uuid is not null then 'Owner'
    when admins.member_uuid is not null then 'Admin'
    when tams.member_uuid is not null then 'TAM'
    when hiring_managers.member_uuid is not null then 'Hiring Manager'
    when people_managers.member_uuid is not null then 'People Manager'
    else 'Other' end as User_Type
  , eho.country
  , eho.industry
  from user_list a
  left join owners on a.employer_member_uuid = owners.member_uuid and a.org_uuid = owners.org_uuid
  left join admins on a.employer_member_uuid = admins.member_uuid and a.org_uuid = admins.org_uuid
  left join tams on a.employer_member_uuid = tams.member_uuid and a.org_uuid = tams.org_uuid
  left join hiring_managers on a.employer_member_uuid = hiring_managers.member_uuid and a.org_uuid = hiring_managers.org_uuid
  left join people_managers on a.employer_member_uuid = people_managers.member_uuid and a.org_uuid = people_managers.org_uuid
  left join {{ref('employment_hero_organisations')}} eho on a.org_uuid = eho.uuid