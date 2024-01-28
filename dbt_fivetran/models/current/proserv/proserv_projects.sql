{{ config(alias='projects') }}

with incomplete_projects as (
    select
      pt.project_id
      , t.name
    from
      asana.project_task pt
      join asana.task t on
        t.id = pt.task_id
    where
      t.name ~ '(Project Complete|Complete project|Close project)'
      and not isnull(t._fivetran_deleted, 'f')
      and t.completed_at is null
  )
  , project_start_date as (
    select
      pt.project_id
      , case 
          when length(t.custom_completed_date) = 8 and regexp_instr(t.custom_completed_date, '/', 1, 1) = 3 then to_date(t.custom_completed_date, 'dd/mm/yy')
          when length(t.custom_completed_date) = 8 and regexp_instr(t.custom_completed_date, '/', 1, 1) = 2 then to_date(t.custom_completed_date, 'd/m/yyyy')
          when length(t.custom_completed_date) = 10 then to_date(t.custom_completed_date, 'dd/mm/yyyy')
        else t.due_on
      end as custom_start_date
      , coalesce(custom_start_date, completed_at, null) as start_date
    from
      asana.project_task pt
      join asana.task t on
        t.id = pt.task_id
    where
      t.name ~ 'Project Initiation'
      and not isnull(t._fivetran_deleted, 'f')
  )

    select distinct
      imps.id
      , imps.name
      , coalesce(imps.project_started_date_c, imps.start_date_c, sd_task.start_date, null) as start_date    
      , coalesce(imps.project_completion_date_c, imps.go_live_date_c, null) as completed_date
      , a.id as account_id
      , a.name as account_name
      , a.geo_code_c as geo_code
      , cg.name as country   
      , su.name as proserv_project_owner
      , case
          when imps.service_offering_c ~ 'Guided HR' then 'HR'
          when imps.service_offering_c ~ 'Managed HR' then 'Managed HR'
          when imps.service_offering_c ~ 'Guided Payroll' then 'Payroll'
          when imps.service_offering_c ~ 'Managed Payroll' then 'Managed Payroll'
          else imps.service_offering_c
        end as service_offering   
      , case 
          when p.name like '%(GP+)%' then true
          else false
        end plus_implementation
      , case
          when imps.service_offering_c ~ 'UK' then 'UK'
          else 'APAC'
        end as region 
      , imps.stage_c as stage
      , imps.status_c as status 
      , case
          when (imps.status_c ~ '(Live|Completed)' or imps.stage_c ~ '(Live|Completed)') and e.approval_status = 'rejected' then 'Ineligible'
          when (imps.status_c ~ '(Live|Completed)' or imps.stage_c ~ '(Live|Completed)') and e.approval_status = 'approved' then 'Eligible'
          else 'Ineligible'
        end as eligibility
      , p.id as asana_project_id
      , p.name as asana_project_name
      , case
          when s.color = 'green' then 'On track'
          when s.color = 'yellow' then 'At risk'
          when s.color = 'red' then 'Off track'
          when s.color = 'blue' then 'On hold'
          when s.color is null then 'Status not provided'
          else initcap(s.color)
        end as asana_project_status
      , t.name as asana_team
      , u.name as asana_project_owner
      , r.role
      , initcap(ltrim(rtrim(p.custom_overdue_theme))) as overdue_theme
      , extract(day from coalesce(completed_date::timestamp, getdate()) - start_date::timestamp) as project_duration_in_day
      , case 
          when project_duration_in_day <= 56 then '<= 8 weeks'
          when project_duration_in_day between 57 and 84 then '8-12 weeks'
          when project_duration_in_day between 85 and 140 then '12-20 weeks'
          when project_duration_in_day > 140 then '>20 weeks'
        end as age_group
      , case
          when imps.service_offering_c ~ 'Guided' and plus_implementation and project_duration_in_day > 56 then 'Overdue'
          when imps.service_offering_c ~ 'Guided' and not plus_implementation and project_duration_in_day > 45 then 'Overdue'
          else 'On Track'
        end as project_management
    
      , case when ip.project_id is null then true else false end project_complete
      , p.created_at as created_at
      , p.due_date as due_date
      , p.current_status as status_update
      , p.custom_ext_stakeholder as ext_stakeholder
      , p.custom_payroll_specialist as asana_payroll_specialist
      , u.email as asana_project_owner_email
      , imps.opportunity_c as opportunity_id
    from
      salesforce.implementation_project_c as imps
      left join salesforce.user su on
        imps.project_owner_c = su.id
        and not su._fivetran_deleted
      left join salesforce.account as a on
        imps.account_c = a.id  
        and not a.is_deleted
      left join csv.country_geo_location as cg on 
        case when a.geo_code_c = 'UK' then 'GB' else a.geo_code_c end = cg.country
      left join salesforce.asana_public_asana_projects_relation_c as ap on
        imps.id = ap.asana_public_object_id_c
        and not ap.is_deleted
        and not ap.asana_public_is_deleted_c
      left join asana.project as p on
        ap.asana_public_asana_project_id_c = p.id
        and not p._fivetran_deleted
      left join asana.team as t on
        p.team_id = t.id
        and not t._fivetran_deleted
      left join asana.user u on
        p.owner_id = u.id
        and not u._fivetran_deleted
      left join proserv.roles r on
        lower(u.email) = lower(r.email)
      left join (
                  select *
                  from workshop_public.asana_project_statuses
                  where id in ( select
                                  FIRST_VALUE(id) over (
                                  partition by asana_project_id order by created_at desc rows between unbounded preceding and current row )
      from
        workshop_public.asana_project_statuses
      where not _fivetran_deleted
    )
) as s on
        p.id = s.asana_project_id
      left join incomplete_projects as ip on
        p.id = ip.project_id
      left join project_start_date as sd_task on
        p.id = sd_task.project_id
      left join {{ ref('proserv_eligibility_milestone') }} as e on
        p.id = e.project_id
    where
      not imps.is_deleted
      and imps.service_offering_c is not null
      and a.name !~* 'test'