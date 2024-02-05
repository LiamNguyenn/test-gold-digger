

select
  account_name                                 as sf_account_name,
  professional_service_name                    as sf_professional_service_name,
  project_started_date                         as sf_project_started_date,
  expected_go_live                             as sf_expected_go_live,
  project_completion_date                      as sf_project_completion_date,
  closed_status                                as sf_closed_status,
  eh_org_id                                    as sf_org_id,
  eh_org_subplan,
  opportunity_employees                        as sf_opportunity_employees,
  employees_created                            as hr_platform_employees_created,
  employees_invited                            as hr_platform_employees_invited,
  employees_activated                          as hr_platform_employees_activated,
  hr_70pc_employees_created_at::date,
  hr_70pc_employees_invited_at::date,
  hr_70pc_employees_activated_at::date,
  first_announcement_at::date                  as hr_platform_first_announcement_at,
  first_company_values_created_at::date        as hr_platform_first_company_values_created_at,
  first_custom_survey_at::date                 as hr_platform_first_custom_survey_at,
  first_happiness_survey_at::date              as hr_platform_first_happiness_survey_at,
  first_document_uploaded_at::date             as hr_platform_first_document_uploaded_at,
  first_certification_created_at::date         as hr_platform_first_certification_created_at,
  first_policy_added_at::date                  as hr_platform_first_policy_added_at,
  first_onboarding_checklist_created_at::date  as hr_platform_first_onboarding_checklist_created_at,
  first_performance_review_created_at::date    as hr_platform_first_performance_review_created_at,
  first_asset_created_at::date                 as hr_platform_first_asset_created_at,
  first_coaching_session_created_at::date      as hr_platform_first_coaching_session_created_at,
  first_okr_created_at::date                   as hr_platform_first_okr_created_at,
  first_custom_security_group_created_at::date as hr_platform_first_custom_security_group_created_at
from "dev"."proserv"."guided_hr_milestones"
where
  eh_org_id is not null
  and sf_org_id is not null