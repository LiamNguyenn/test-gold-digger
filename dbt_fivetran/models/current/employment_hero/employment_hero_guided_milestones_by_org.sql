{{
    config(       
        alias='guided_milestones_by_org'
    )
}}

with custom_surveys as (
  select o.id as organisation_id, min(c.updated_at) as first_custom_survey_at
  from {{source('survey_services_public', 'custom_surveys')}} as c
  join {{ref('employment_hero_organisations')}} as o on c.organisation_id = o.uuid
  --where not c._fivetran_deleted
  group by o.id
),
  
happiness_surveys as (
  select o.id as organisation_id, min(h.updated_at) as first_happiness_survey_at
  from {{source('survey_services_public', 'happiness_surveys')}} as h
  join {{ref('employment_hero_organisations')}} as o on  h.organisation_id = o.uuid
  --where not h._fivetran_deleted
  group by o.id
),

announcements as (
  select o.id as organisation_id, min(a.created_at) as first_announcement_at
  from {{source('announcement_api_production_public', 'announcements')}} as a
  join {{ref('employment_hero_organisations')}} as o on a.organisation_id = o.uuid
  --where not a._fivetran_deleted
  group by o.id
),

company_values as (
  select o.id, min(cv.created_at) as first_company_values_created_at
  from {{source('nominations_public', 'company_values')}} as cv
  join {{ref('employment_hero_organisations')}} as o on cv.organisation_id = o.uuid
  --where company_values._fivetran_deleted is not null
  group by o.id
),

documents as (
  select min(d.created_at) as first_document_uploaded_at, m.organisation_id
  from {{source('postgres_public', 'uploaded_documents')}} as d
  join {{ref('employment_hero_employees')}} as m on d.member_id = m.id
  --where not d._fivetran_deleted
  group by m.organisation_id
),

certifications as (
  select organisation_id, min(created_at) as first_certification_created_at
  from {{source('postgres_public', 'licences')}}
  --where not _fivetran_deleted  
  group by organisation_id
),

policy as (  
  select organisation_id, min(created_at) as first_policy_added_at
  from  {{source('postgres_public', 'contracts')}} 
  where --not _fivetran_deleted and 
  type = 'Policy'
  and "status" = 'Published' 
  group by organisation_id
),

checklist as (
  select min(created_at) as first_onboarding_checklist_created_at, organisation_id
  from  {{source('postgres_public', 'checklists')}}
  where --not _fivetran_deleted
    type = 'OnboardingChecklist'
  group by organisation_id
),

performance_review as (
  select min(created_at) as first_performance_review_created_at, organisation_id
  from {{source('postgres_public', 'reviews')}} 
  --where not _fivetran_deleted
  group by organisation_id
),

assets as (
  select organisation_id, min(created_at) as first_asset_created_at
  from  {{source('postgres_public', 'asset_items')}} 
  --where not _fivetran_deleted
  group by organisation_id
),

coaching as (
  select min(ooo.created_at) as first_coaching_session_created_at, o.id as organisation_id
  from {{source('meeting_management_public', 'one_on_one_sessions')}} as ooo
  join {{source('meeting_management_public', 'one_on_ones')}} as oo on oo.id = ooo.one_on_one_id
  join {{ref('employment_hero_organisations')}}  as o on oo.organisation_id = o.uuid
  group by o.id
),

okr as (
  select m.organisation_id, min(obj.created_at) as first_okr_created_at
  from {{source('postgres_public', 'okrs_objectives')}} as obj
  join {{ref('employment_hero_employees')}} as m on obj.creator_id = m.id or obj.owner_id = m.id 
  --where not archived  and not obj._fivetran_deleted
  group by m.organisation_id
),

security_group as (
    select * from 
    (
        select organisation_id, 
        created_at as first_custom_security_group_created_at,
        row_number () over (partition by organisation_id order by created_at asc)
        from (
            select
            organisation_id,
            created_at    
            from {{source('postgres_public', 'groups')}}
            union 
            select 
            organisation_id,
            created_at   
            from {{source('postgres_public', 'security_roles')}} 
        )
    )
  where row_number = 2
)

  select o.id as organisation_id,  
  first_announcement_at,
  first_company_values_created_at,
  first_custom_survey_at,
  first_happiness_survey_at,
  first_document_uploaded_at,
  first_certification_created_at,
  first_policy_added_at,
  first_onboarding_checklist_created_at,
  first_performance_review_created_at,
  first_asset_created_at,
  first_coaching_session_created_at,  
  first_okr_created_at,
  first_custom_security_group_created_at
  from {{ref('employment_hero_organisations')}} as o  
  left join announcements as anc on o.id = anc.organisation_id
  left join custom_surveys as cs on o.id = cs.organisation_id
  left join happiness_surveys as hs on o.id = hs.organisation_id
  left join company_values on o.id = company_values.id
  left join documents on o.id = documents.organisation_id
  left join certifications on o.id = certifications.organisation_id
  left join policy on o.id = policy.organisation_id
  left join checklist on o.id = checklist.organisation_id
  left join performance_review on o.id = performance_review.organisation_id
  left join assets on o.id = assets.organisation_id
  left join coaching on o.id = coaching.organisation_id  
  left join okr on o.id = okr.organisation_id
  left join security_group on o.id = security_group.organisation_id