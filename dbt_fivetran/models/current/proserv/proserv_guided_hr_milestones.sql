{{
    config(
        materialized='table',
        alias='guided_hr_milestones'
    )
}}

with account as (
    select 
    distinct a.id as account_id, 
    a.name as account_name, 
    ipc.name as professional_service_name,
    o.org_id_c as org_id, 
    eho.sub_name as eh_org_subplan,
    --o.name as org_name, -- some orgs on SF don't have org ID
    opp.id as opp_id,
    opp.close_date, 
    nullif(opp.opportunity_employees_c, 0) as opportunity_employees, 
    round(opportunity_employees_c*0.7,0) as seventy_percent_opp_emps,
    ipc.service_offering_c,
    ipc.project_started_date_c,
    ipc.expected_go_live_c,
    ipc.project_completion_date_c,
    ipc.closed_status_c
    --, p_2.name as product_name
    from {{source('salesforce', 'account')}} as a
    join {{source('salesforce', 'implementation_project_c')}} as ipc on ipc.account_c = a.id        
    left join {{source('salesforce', 'eh_org_c')}} as o on o.professional_service_project_c is not null and ipc.id = o.professional_service_project_c and not o.is_deleted
    left join {{ref('employment_hero_organisations')}} as eho on o.org_id_c = eho.id
    left join {{source('salesforce', 'opportunity')}} as opp on ipc.opportunity_c is not null and ipc.opportunity_c = opp.id and not opp.is_deleted and opp.stage_name = 'Won'
    where not a.is_deleted
        and not ipc.is_deleted
        and ipc.service_offering_c ~* '(guided hr|combined journey)'
        --and (ipc.stage_c != 'Cancelled' or ipc.stage_c is null)
        --and (a.customer_stage_c !~ '(Churned|Lost Before Activated)' or a.customer_stage_c is null)
)

, employee_status as (
    select organisation_id
    , sum(1) as employees_created
    , sum(case when invited_at is not null then 1 else 0 end) as employees_invited
    , sum(case when activated_at is not null then 1 else 0 end) as employees_activated
    , min(case when create_order >= seventy_percent_opp_emps then created_at end) as hr_70pc_employees_created_at
    , min(case when invite_order >= seventy_percent_opp_emps then invited_at end) as hr_70pc_employees_invited_at
    , min(case when activate_order >= seventy_percent_opp_emps then activated_at end) as hr_70pc_employees_activated_at
    from account as a
    join {{ref('employment_hero_employee_status_by_org')}} eo on eo.organisation_id = a.org_id    
    group by 1
)
select 
    {{ dbt_utils.generate_surrogate_key(['a.account_id', 'a.org_id', 'a.professional_service_name']) }} as id    
    , a.account_id
    , a.account_name
    , a.service_offering_c
    , a.professional_service_name
    , a.project_started_date_c as project_started_date
    , a.expected_go_live_c as expected_go_live
    , a.project_completion_date_c as project_completion_date
    , a.closed_status_c as closed_status
    , a.org_id as eh_org_id
    , a.eh_org_subplan
    , a.opportunity_employees
    , eo.employees_created
    , eo.employees_invited
    , eo.employees_activated
    , eo.hr_70pc_employees_created_at
    , eo.hr_70pc_employees_invited_at
    , eo.hr_70pc_employees_activated_at
    , gmo.first_announcement_at
    , gmo.first_company_values_created_at
    , gmo.first_custom_survey_at
    , gmo.first_happiness_survey_at
    , gmo.first_document_uploaded_at
    , gmo.first_certification_created_at
    , gmo.first_policy_added_at
    , gmo.first_onboarding_checklist_created_at
    , gmo.first_performance_review_created_at
    , gmo.first_asset_created_at
    , gmo.first_coaching_session_created_at
    , gmo.first_okr_created_at
    , gmo.first_custom_security_group_created_at
from account as a
left join employee_status eo on a.org_id is not null and eo.organisation_id = a.org_id
left join {{ref('employment_hero_guided_milestones_by_org')}} gmo on a.org_id is not null and gmo.organisation_id = a.org_id 
