

with shown as

(
select 
employer_member_uuid
, user_uuid
, org_uuid
, job_matched
, 'Shown' as Funnel_Stage
, min(CAST(COALESCE(first_shown_at, first_previewed_at, saved_or_shortlisted_at, hired_at, onboarded_at) as DATE)) as Action_Date
, sum(No_Of_Shown_Actions) as No_Of_Actions
from "dev"."ats"."smart_match_employer_actions"
where COALESCE(first_shown_at, first_previewed_at, saved_or_shortlisted_at, hired_at, onboarded_at) is not null
group by 1, 2, 3, 4, 5
)
,

previewed as

(
select 
employer_member_uuid
, user_uuid
, org_uuid
, job_matched
, 'Previewed' as Funnel_Stage
, min(CAST(COALESCE(first_previewed_at, saved_or_shortlisted_at, hired_at, onboarded_at) as DATE)) as Action_Date
, sum(No_Of_Previewed_Actions) as No_Of_Actions
from "dev"."ats"."smart_match_employer_actions"
where COALESCE(first_previewed_at, saved_or_shortlisted_at, hired_at, onboarded_at) is not null
group by 1, 2, 3, 4, 5
)
,
saved as

(
select 
employer_member_uuid
, user_uuid
, org_uuid
, job_matched
, 'Saved' as Funnel_Stage
, min(CAST(COALESCE(saved_or_shortlisted_at, hired_at, onboarded_at) as DATE)) as Action_Date
, sum(No_Of_Saved_Actions) as No_Of_Actions
from "dev"."ats"."smart_match_employer_actions"
where COALESCE(saved_or_shortlisted_at, hired_at, onboarded_at) is not null
group by 1, 2, 3, 4, 5
)
,

shortlisted as

(
select 
employer_member_uuid
, user_uuid
, org_uuid
, job_matched
, 'Shortlisted' as Funnel_Stage
, min(CAST(COALESCE(shortlisted_at, hired_at, onboarded_at) as DATE)) as Action_Date
, sum(No_Of_Shortlisted_Actions) as No_Of_Actions
from "dev"."ats"."smart_match_employer_actions"
where COALESCE(shortlisted_at, hired_at, onboarded_at) is not null
group by 1, 2, 3, 4, 5
)
,

onboarded as

(
select 
employer_member_uuid
, user_uuid
, org_uuid
, job_matched
, 'Onboarded' as Funnel_Stage
, min(CAST(COALESCE(hired_at, onboarded_at) as DATE)) as Action_Date
, sum(No_Of_Onboarded_Actions) as No_Of_Actions
from "dev"."ats"."smart_match_employer_actions"
where COALESCE(hired_at, onboarded_at) is not null
group by 1, 2, 3, 4, 5
)

select
a.employer_member_uuid
, a.user_uuid
, a.org_uuid
, a.job_matched
, a.Funnel_Stage
, a.Action_Date
, a.No_Of_Actions
from
(select * from shown
 union
 select * from previewed
 union
 select * from saved
 union
 select * from shortlisted
 union
 select * from onboarded) a