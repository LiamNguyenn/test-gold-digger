

with
  all_events as (
    select
        e.message_id
        , e.timestamp
        , e.name
        , case 
            when module != '' and e.name != 'Visit Company Feed' then module
            when app_version_string is not null then 'mobile' 
            else 'others' end as module
        , (case when sub_module= '' then null else sub_module end) as sub_module
        , regexp_substr(e.name, '[^#]*$') as mobile_page
        -- general user details  
        , case
        when trim(user_id) ~ '^[0-9]+$' then trim(user_id)
        else null
    end::int as numeric_user_id
        , (case when eh_user_type= '' then null else eh_user_type end) as eh_persona
        , (case when login_provider= 'kp' then 'kp' else 'eh' end) as login_provider
        -- EH user details if EH event
        , (case when email= '' then null else email end) as email
        , coalesce(
            (case when user_uuid= '' then null else user_uuid end),
            (case when user_id= '' then null else user_id end)
         ) as user_id
        , (case when member_id= '' then null else member_id end) as member_id
        , (case when member_uuid= '' then null else member_uuid end) as member_uuid
        , case
        when trim(organisation_id) ~ '^[0-9]+$' then trim(organisation_id)
        else null
    end::int as organisation_id
        , (case when user_type= '' then null else user_type end) as user_type
        -- Keypay user details if KP event
        , (case when user_email= '' then null else user_email end) as kp_email
        , case
        when trim(kp_employee_id) ~ '^[0-9]+$' then trim(kp_employee_id)
        else null
    end::int as kp_employee_id
        , case
        when trim(kp_business_id) ~ '^[0-9]+$' then trim(kp_business_id)
        else null
    end::int as kp_business_id
        , (case when kp_user_type= '' then null else kp_user_type end) as kp_user_type
        -- misc event details  
        , (case when platform= '' then null else platform end) as platform
        , e.os
        , e.device  
        , e.browser
        , e.screen_width
        , e.screen_height
        , e.screen_dpi
        , e.app_version_string
        , e.shopnow_offer_module
        , e.shopnow_offer_type
        , e.shopnow_offer_category

    from
        "dev"."customers"."int_events" e
  )

, members as (
    select 
      m.uuid as member_uuid, m.is_shadow_data as member_is_shadow_data, m._fivetran_deleted as member_fivetran_deleted, m.system_manager, m.system_user
     , u.email, u.uuid as user_uuid, u.id as user_id, u.is_shadow_data as u_is_shadow_data, u._fivetran_deleted as u_fivetran_deleted
    from 
      "dev"."postgres_public"."users" u
      join "dev"."postgres_public"."members" m on 
        u.id = m.user_id
  		and m.uuid != 'ff570046-b394-4c62-9bf9-aaa93f4b6a8f' -- duplicate m.uuid, these are internal members
  )

select
  e.message_id
  , e.timestamp
  , e.name
  , e.module
  , e.sub_module
  , e.mobile_page
  , e.login_provider
  , e.numeric_user_id
  , e.member_id
  , e.member_uuid
  , e.organisation_id
  , e.user_type
  , e.kp_employee_id
  , e.kp_business_id
  , e.kp_user_type
 , coalesce(e.user_id, u.uuid, m.user_uuid, e.login_provider || '-' || e.numeric_user_id ) as user_id -- need this for vitally
  , coalesce(e.email, e.kp_email, kp_user.email, kp_employee.email, u.email, u2.email, m.email)  as user_email
  , case 
  	when e.login_provider = 'kp' then 'WZ User' 
  	when e.login_provider = 'eh' and eh_persona ilike '%candidate%' then 'Candidate'
	else 'EH Employee' end as persona
  , e.platform
  , e.os
  , e.device  
  , e.browser
  , e.screen_width
  , e.screen_height
  , e.screen_dpi
  , e.app_version_string
  , e.shopnow_offer_module
  , e.shopnow_offer_type
  , e.shopnow_offer_category
from 
  all_events e
  left join "dev"."keypay"."user" kp_user on
    e.numeric_user_id = kp_user.id
    and login_provider = 'kp'
  left join "dev"."keypay_dwh"."employee" kp_employee on
   	e.kp_employee_id = kp_employee.id
  	and login_provider = 'kp'
  left join "dev"."postgres_public"."users" u on
    e.numeric_user_id = u.id
    and login_provider = 'eh'
  left join "dev"."postgres_public"."users" u2 on
    e.user_id = u2.uuid
    and login_provider = 'eh'
  left join members m on
    e.member_uuid = m.member_uuid
    and login_provider = 'eh'
 where
    (user_email !~* '.*(employmenthero|keypay|employmentinnovations|thinkei|p2m|power2motivate|test|demo|exacc|sandbox|\\+).*')
   and (system_manager is null or not system_manager)
   and (system_user is null or not system_user)

       
     and e.timestamp > (SELECT MAX(timestamp) FROM "dev"."customers"."events")
