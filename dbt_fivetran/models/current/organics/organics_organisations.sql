{{ config(alias='organisations') }}

with 
  org_creator as (
  select
    organisation_id
    , member_id
    , user_email
  from
    (
      select
      m.organisation_id
      , m.id as member_id
      , u.email as user_email
      , row_number() over (partition by m.organisation_id order by m.created_at) as rn
      from
        {{ source('postgres_public', 'members') }} as m
      join {{ source('postgres_public', 'users') }} as u on
        m.user_id = u.id
      where
        not m._fivetran_deleted        
        and not u._fivetran_deleted
        and not m.is_shadow_data
        and not u.is_shadow_data
      )
  where rn = 1 
    )
  , organics_activated as (
    select
      organisation_id
      ,convert_timezone('Australia/Sydney', completed_at) as activated_at
    from
      {{ source('postgres_public', 'organisation_guides') }} og
      join {{ source('postgres_public', 'guides') }} g on
        og.guide_id = g.id
    where
      not og._fivetran_deleted
      and not g._fivetran_deleted
      and g.type = 'SetupGuide'
      and g.name = 'launch'
  )
  , playground as (
    select
      organisation_id
      ,min(time) as first_playground_activity
      ,max(time) as last_playground_activity
      ,count(event_name) as num_playground_activity
    from
      (
      select
        pa.*
        ,m.organisation_id
        ,convert_timezone('Australia/Sydney', o.created_at) as org_created_at 
      from 
        {{ ref('mp_playground_activities') }} pa
        join {{ source('postgres_public', 'members') }} m
          on pa.user_id = m.user_id
        join {{ source('postgres_public', 'users') }} u
          on m.user_id = u.id
        join {{ source('postgres_public', 'organisations') }} o 
          on m.organisation_id = o.id
      where
        email !~* '.*(employmenthero|employmentinnovations|thinkei|p2m|power2motivate|test|demo|exacc|sandbox|\\+).*'
        and not m.system_manager
        and not m.system_user
        and not m.independent_contractor
        and not m._fivetran_deleted
        and not o._fivetran_deleted
        and not u._fivetran_deleted
        and not m.is_shadow_data
        and not u.is_shadow_data
        and not o.is_shadow_data
        -- playground open for only 14 days from creation date
        and pa.time >= org_created_at
        and pa.time <= dateadd(days, 14, org_created_at)
        )
    group by 1
  )
  , platinum_trial as (
    select
      a.organisation_id
      , convert_timezone('Australia/Sydney', trials.created_at) as platinum_trial_created_at
    from
      (
        select *
        from {{ source('postgres_public', 'agreements') }}
        where id in (
          select
            FIRST_VALUE(id) over (partition by organisation_id order by created_at asc rows between unbounded preceding and current row)
          from
            {{ source('postgres_public', 'agreements') }}
        )
      ) as a
      join (
        select *
        from {{ source('postgres_public', 'trial_opt_ins') }}
        where id in (
          select
            FIRST_VALUE(id) over (partition by agreement_id order by created_at asc rows between unbounded preceding and current row)
          from
            {{ source('postgres_public', 'trial_opt_ins') }}
        )
      ) as trials on
        a.id = trials.agreement_id
      join {{ref('employment_hero_v_sub_plan_grouping')}} as s on
        trials.subscription_plan_id = s.id
    where
      not a._fivetran_deleted
      and not trials._fivetran_deleted
      and s.pricing_tier = 'platinum'
  )
  , guides as (
    select 
      organisation_id
      ,min(guide_completed_at) earliest_guide_completed_at
      ,max(guide_completed_at) most_recent_guide_completed_at
      ,count(distinct (case when guide_completed_at is not null then organisation_guides_id end)) as number_of_guides_completed
    from 
      {{ ref('employment_hero_guides') }}
    group by 1
  )  
  , company_setup_wizards as (
    -- Generally each org should only have 1 record each in this table. However, there are 199 organisations with multiple records so we're using the earliest created record
    select organisation_id
      , created_at
      , updated_at

    from (
      select organisation_id
        , created_at
        , updated_at
        , row_number() over (partition by organisation_id order by created_at) as rn

      from {{ source('postgres_public', 'company_setup_wizards') }}

      where not _fivetran_deleted)

    where rn = 1
  )
  , converted as (
    -- updated to include a flag on whether the org is converted in demo or real org account
    -- logic is provided by @Long Nguyen https://employmenthero.slack.com/archives/C066X19E11S/p1700705904036499 (Kevin's Slack account)
    select
      c.organisation_id      
      , c.converted_at
      , (csw.updated_at is not null and c.converted_at < csw.updated_at and datediff('second', csw.created_at, csw.updated_at) > 3) as converted_in_demo_account
      , c.pricing_tier as first_paid_tier
    from (
        select a.organisation_id      
        , convert_timezone('Australia/Sydney', a.created_at) as converted_at
        , s.pricing_tier 
        , row_number() over (partition by a.organisation_id ORDER BY a.created_at) rn       
        from 
        {{ source('postgres_public', 'agreements') }} a 
        join {{ref('employment_hero_v_sub_plan_grouping')}} as s on
          a.subscription_plan_id = s.id
        where
          not a._fivetran_deleted
          and s.pricing_tier != 'free'        
      ) c
    left join company_setup_wizards as csw
      on csw.organisation_id = c.organisation_id
    where c.rn = 1
  )
  , platinum_trial_signup_journey as (
    -- Platinum trial sign up flow per https://employmenthero.atlassian.net/wiki/spaces/FP/pages/2755658978/2023-11+Selected+Business+goal+in+sign+up+journeys
    select osa.id
      , osa.member_id
      , m.organisation_id
      , osa.user_id
      , json_parse(osa.details) as details
      , osa.created_at
      , osa.updated_at
    
    from {{ source('postgres_public', 'onboarding_survey_answers') }} osa
    
    inner join {{ source('postgres_public', 'members') }} m
      on m.id = osa.member_id
    
    where not osa._fivetran_deleted
      and not m._fivetran_deleted
      and not m._fivetran_deleted
      and not m.is_shadow_data
  )
  , goal as (
    select *

    from (
      select *
        , row_number() over (partition by organisation_id order by goal_answer_created_at) as rn         
      from (
        -- Basic sign up flow per https://employmenthero.atlassian.net/wiki/spaces/FP/pages/2755658978/2023-11+Selected+Business+goal+in+sign+up+journeys
        select o.id as organisation_id
        , q.question_text
        , cc.value as goal_on_signup
        , a.created_at as goal_answer_created_at
        from      
          {{ source('survey_services_public', 'single_choice_answer_contents') }} scac     
          join {{ source('survey_services_public', 'choice_contents') }} cc on cc.id = scac.choice_content_id
          join {{ source('survey_services_public', 'answer_details') }} ad on scac.id = ad.content_id and ad.content_type = 'SingleChoiceAnswerContent'
          join {{ source('survey_services_public', 'answers') }} a on a.id = ad.answer_id
          join {{ source('survey_services_public', 'surveys') }} s on a.survey_id = s.id
          join {{ source('survey_services_public', 'questions') }} q on q.id = ad.question_id
          join {{ source('survey_services_public', 'onboarding_surveys') }} os on os.survey_id = s.id
          join {{ source('survey_services_public', 'members') }} as m on m.id = a.member_id      
          join {{ source('postgres_public', 'organisations') }} as o on m.organisation_id = o.uuid      
        where q.question_text = 'What do you want to get from Employment Hero?'
          and not scac._fivetran_deleted
          and not cc._fivetran_deleted
          and not ad._fivetran_deleted
          and not a._fivetran_deleted
          and not s._fivetran_deleted
          and not q._fivetran_deleted
          and not os._fivetran_deleted
          and not m._fivetran_deleted
          and not o._fivetran_deleted
          and not o.is_shadow_data
        --where (cc.value = 'Reduce administration time' or  cc.value = 'Simplify and use one platform' or  cc.value = 'Organise my employee data' or cc.value = 'Keep remote employees engaged' or  cc.value = 'Engage my employees' or cc.value = 'Get my bussiness up and running' or cc.value ='Get access to Policies & Contracts') 

        union 

        -- Platinum trial sign up flow per https://employmenthero.atlassian.net/wiki/spaces/FP/pages/2755658978/2023-11+Selected+Business+goal+in+sign+up+journeys
        select pt.organisation_id
          , array.question::varchar as question_text
          , regexp_replace(json_serialize(array.answers), '"|\\[|\\]', '') as goal_on_signup
          , pt.created_at as goal_answer_created_at

        from platinum_trial_signup_journey pt
          , pt.details as array
        )
    )
    where rn = 1
  )
  , payroll_platform_currently_using as (
  select * 
  from 
    (
    select
--       m.id as member_id
      o.id as organisation_id
--       , a.created_at
      , cc.value as payroll_on_signup
      , row_number() over (partition by o.id order by a.created_at) as rn
    from 
      {{ source ('survey_services_public', 'onboarding_surveys') }} os
      join {{ source('survey_services_public', 'questions') }} q
        on os.survey_id = q.survey_id
      join {{ source('survey_services_public', 'answers') }} a
        on q.survey_id = a.survey_id
      join {{ source('survey_services_public', 'answer_details') }} ad
        on a.id = ad.answer_id and q.id=ad.question_id
      join {{ source('survey_services_public', 'single_choice_answer_contents') }} scac
        on ad.content_id = scac.id
      join {{ source('survey_services_public', 'choice_contents') }} cc 
        on cc.id = scac.choice_content_id
      join {{ source('survey_services_public', 'members') }} m 
        on m.id = a.member_id      
      join {{ source('postgres_public', 'organisations') }} o 
        on m.organisation_id = o.uuid
    where
      (q.question_text = 'What payroll platform are you currently using?' 
      or q.question_text = 'What payroll do you want to connect to?')
      and cc.value in ('Employment Hero Payroll', 'KeyPay', 'QuickBooks Online', 'Xero', 'MYOB', 'Other')
--       and os.status = 'active'
      and not os._fivetran_deleted
      and not q._fivetran_deleted
      and not a._fivetran_deleted
      and not ad._fivetran_deleted
      and not scac._fivetran_deleted
      and not cc._fivetran_deleted
      and not o.is_shadow_data
  --     and not m._fivetran_deleted
  --     and not o._fivetran_deleted
    )
  where rn=1  
  )
  , emp_count as (
  select 
    organisation_id
    , count(*) as number_of_active_members
  from {{ ref('employment_hero_employees') }}
  where active
  group by 1
  )
  , upsold as (
    select * from (
      select
        *
        , row_number() over (partition by organisation_id order by upsold_at) as rn
      from
        (
          select
            o.id as organisation_id
            , convert_timezone('Australia/Sydney', a.created_at) as upsold_at
            , s.name as subscription
            , s.pricing_tier 
            , s.pricing_type
            , s.pricing_hierarchy
            , lag(s.name)over (partition by o.id order by a.created_at asc) as prev_subscription
            , lag(s.pricing_hierarchy) over (partition by o.id order by a.created_at asc) as prev_pricing_hierarchy
            , s.pricing_hierarchy - prev_pricing_hierarchy as change_pricing_hierarchy
          from
            {{ source('postgres_public', 'organisations') }} as o
            join {{ source('postgres_public', 'agreements') }} as a on
              a.organisation_id = o.id
            join {{ref('employment_hero_v_sub_plan_grouping')}} as s on
              a.subscription_plan_id = s.id
          where
            not o._fivetran_deleted
            and not o.is_shadow_data
            and not a._fivetran_deleted
            and s.pricing_hierarchy != 0
          )
      where change_pricing_hierarchy>0
      )
    where rn=1
  )

select 
  o.id
  --, o.uuid
  , o.name as org_name
  , ic.title as industry
--  , case    when o.name is null    or o.name = ''      then e.name    else o.name  end as org_name
  , convert_timezone('Australia/Sydney', o.created_at) as created_at
  , oc.user_email as creator_email
  , o.setup_mode
  , o.estimated_number_of_employees
  , ec.number_of_active_members
  --, s.id as current_sub_id
  , s.name as current_subscription
  , s.pricing_tier as current_pricing_tier
  --, a.created_at as subscribed_at
  , s.pricing_type
  , za.id as zuora_account_id
  , za.account_number as zuora_account_number
  , za.batch as zuora_batch
  , pr.having_payroll_not_from_sign_up 
  , o.country
  , dc.partner_name as discount_partner
  , pg.first_playground_activity
  , pt.platinum_trial_created_at
  , act.activated_at
  , gs.earliest_guide_completed_at
  , gs.most_recent_guide_completed_at
  , c.converted_at
  , c.converted_in_demo_account
  , c.first_paid_tier
  , csw.updated_at > csw.created_at as activated_real_org
  , u.upsold_at as first_upsold_at
  , g.goal_on_signup
  , pp.payroll_on_signup
  , gs.number_of_guides_completed
  , daumau.monthly_users
from
  {{ source('postgres_public', 'organisations') }} as o
  join (
        select *
          from {{ source('postgres_public', 'agreements') }}
          where id in (
            select
              FIRST_VALUE(id) over (partition by organisation_id order by created_at desc rows between unbounded preceding and current row)
            from
              {{ source('postgres_public', 'agreements') }}
            where not _fivetran_deleted
          )
      ) as a on
    o.id = a.organisation_id
  join {{ref('employment_hero_v_sub_plan_grouping')}} as s on
    a.subscription_plan_id = s.id
  join org_creator oc on 
    oc.organisation_id = o.id
  left join {{ source('postgres_public', 'industry_categories') }} as ic
    on o.industry_category_id = ic.id
  left join emp_count as ec on ec.organisation_id = o.id
  left join {{ source('zuora', 'account') }} za on za.id = o.zuora_account_id and not za._fivetran_deleted 
  left join {{ source('postgres_public', 'discount_codes') }} dc on 
    dc.id = o.discount_code_id and not dc._fivetran_deleted
  left join (select organisation_id, case when sum(case when not connect_through_sign_up then 1 else 0 end)>0 then true else false end as having_payroll_not_from_sign_up 
             from {{ source('postgres_public', 'external_payroll_auths') }} 
             where not _fivetran_deleted 
             group by 1) pr on pr.organisation_id = o.id
  left join organics_activated act on
    o.id = act.organisation_id
  left join playground pg on
    o.id = pg.organisation_id
  left join platinum_trial pt on 
    o.id = pt.organisation_id
  left join guides gs on
    o.id = gs.organisation_id
-- hygiene issue: multiple default entities
--  left join postgres_public.employing_entities as e on    o.id = e.organisation_id    and e."default"    and e."enable"    and not e._fivetran_deleted
  left join converted c on 
    o.id = c.organisation_id
  left join upsold u on
    o.id = u.organisation_id
  left join goal g on g.organisation_id = o.id
  left join payroll_platform_currently_using pp on o.id = pp.organisation_id
  left join {{ ref('mp_daumau_by_org') }} daumau on daumau.organisation_id = o.id and daumau.date::date = getdate()::date
  left join company_setup_wizards csw
    on csw.organisation_id = o.id
where
  not o._fivetran_deleted
  and not o.is_shadow_data
  and
(
  (s.pricing_type = 'organic' 
   and o.zuora_account_id is not null 
   and o.business_account_id is null 
   and s.name not ilike '%reseller%')
  or 
    ( --s.id = 11 -- free sub plan 
      s.pricing_tier = 'free'
      and s.name not in ('SUSPENDED (FREE)','CHURN (FREE)')
      and o.estimated_number_of_employees < 10)
)
and ( o.discount_code_id is null or dc.partner_name != 'tester' )
and creator_email !~* '.*(employmenthero|employmentinnovations|thinkei|p2m|power2motivate|test|demo|exacc|sandbox|\\+).*'