


with owners as (
  select distinct o.country, m.user_uuid, m.uuid as member_uuid, o.uuid as org_uuid
  from "dev"."employment_hero"."employees" m
  join "dev"."employment_hero"."organisations" o on m.organisation_id = o.id 
  where m.role ilike 'owner'
  and m.user_id is not null
  and m.uuid is not null
  )
  ,
  admins as (
  select distinct o.country, m.user_uuid, m.uuid as member_uuid, o.uuid as org_uuid
  from "dev"."employment_hero"."employees" m
  join "dev"."employment_hero"."organisations" o on m.organisation_id = o.id 
  where m.role ilike 'employer'
  and m.user_id is not null
  and m.uuid is not null
  )
  ,
  hiring_managers as (
  select distinct o.country, m.user_uuid, m.uuid as member_uuid, o.uuid as org_uuid
  from "dev"."ats_public"."hiring_managers" hm
  join "dev"."employment_hero"."employees" m on hm.member_id = m.uuid
  join "dev"."employment_hero"."organisations" o on m.organisation_id = o.id
  join "dev"."ats"."jobs_created" j on hm.job_id = j.job_id
  where m.role not ilike 'owner'
  and m.role not ilike 'employer'
  and hm._fivetran_deleted ='f'
  and m.user_id is not null
  and m.uuid is not null
  )
  ,
  people_managers as (
  select distinct o.country, m.user_uuid, m.uuid as member_uuid, o.uuid as org_uuid
  from "dev"."postgres_public"."member_managers" pm
  join "dev"."employment_hero"."employees" m on pm.manager_id = m.id
  join "dev"."employment_hero"."organisations" o on m.organisation_id = o.id
  where m.role not ilike 'owner'
  and m.role not ilike 'employer'
  and m.user_id is not null
  and not pm._fivetran_deleted
  and m.uuid is not null
  )
  ,
  security_groups as (
  select rp.key, coalesce(ra.member_id, tm.member_id) as member_id, r.is_affecting_all_employees, r.organisation_id
  from "dev"."postgres_public"."security_roles" r
  join "dev"."postgres_public"."security_role_assignees" ra on ra.security_role_id = r.id
  join "dev"."postgres_public"."security_role_permissions" rp on rp.security_role_id = r.id
  join "dev"."postgres_public"."team_members" tm on tm.team_id = ra.team_id
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
  join "dev"."employment_hero"."employees" m on sg.member_id = m.id
  join "dev"."employment_hero"."organisations" o on m.organisation_id = o.id
  where m.role not ilike 'owner'
  and m.role not ilike 'employer'
  and m.user_id is not null
  and m.uuid is not null
  )

  ,

  user_list as (
  select distinct employer_member_uuid, org_uuid from "dev"."ats"."smart_match_employer_actions" where employer_member_uuid is not null and org_uuid is not null)

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
  left join "dev"."employment_hero"."organisations" eho on a.org_uuid = eho.uuid