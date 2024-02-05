

with old_super_events as (
  select  
    "time",
    name,
    json_extract_path_text(properties, 'email') as email,
    json_extract_path_text(properties, 'member_id') as member_id,
    coalesce(user_id, json_extract_path_text(properties, 'user_uuid', true)) as user_id,
    json_extract_path_text(properties, 'organisation_id') as organisation_id,
    case when name ~ 'OnboardingQSuper#SubmitSucceed' then 'superfund' end as survey_type,
    case when name ~ 'OnboardingQSuper#SubmitSucceed' then 'q-super' end as survey_code    
  from "dev"."mp"."event"
  where name ~ '(Onboarding#VisitQSuperDetail|Onboarding#VisitQSuperSurvey|OnboardingQSuper#SubmitSucceed)'
),
onboard_super_events as (
  select  
    "time",
    name,
    json_extract_path_text(properties, 'email') as email,
    json_extract_path_text(properties, 'member_id') as member_id,
    json_extract_path_text(properties, 'user_id', true) as user_id,
    json_extract_path_text(properties, 'organisation_id') as organisation_id,
    json_extract_path_text(properties, 'survey_type') as survey_type,
    json_extract_path_text(properties, 'survey_code') as survey_code
  from "dev"."mp"."event" 
  where json_extract_path_text(properties, 'from_onboarding') = 'true'
   and name ~ '(Visit Super Choice Page|Visit Super Choice Tab|Click View Details)'
   and json_extract_path_text(properties, 'survey_type') ~ '(^$|superfund)'
),  
super_events as (
  select  
    "time",
    name,
    json_extract_path_text(properties, 'email') as email,
    json_extract_path_text(properties, 'member_id') as member_id,
    json_extract_path_text(properties, 'user_id', true) as user_id,
    json_extract_path_text(properties, 'organisation_id') as organisation_id,
    json_extract_path_text(properties, 'survey_type') as survey_type,
    json_extract_path_text(properties, 'survey_code') as survey_code
  from "dev"."mp"."event"   
  where name ~ '(Visit Superfund Vendor Page|Survey Submitted Succeed|Click Apply Now)'
    and json_extract_path_text(properties, 'survey_type') ~ '(^$|qsuper|superfund)'
),
all_super_events as (
  (select *
  from old_super_events)
  union
  (select * 
  from onboard_super_events)
  union
  (select * 
  from super_events)
)

select *
from all_super_events 

    where "time" > (select max(time) from "dev"."mp"."super_event" )
