-- Note: Required snapshot properties may not work when defined in config YAML blocks. We recommend that you define these in dbt_project.yml or a config() block within the snapshot .sql file. ref: https://docs.getdbt.com/reference/resource-configs/grants
{% snapshot marketing_users_snapshot %}

{{
    config(
    alias='users_snapshot',
    target_schema="marketing",
    strategy='check',
    unique_key='eh_platform_user_id',
    check_cols='all',
    invalidate_hard_deletes=True,
    grants = {'select': ['marketo']},
    post_hook = "delete from {{ this }} where dbt_scd_id not in (select FIRST_VALUE(dbt_scd_id) over(partition by eh_platform_user_id order by dbt_updated_at desc rows between unbounded preceding and unbounded following) from {{ this }})"
    )
}}

with 
current_employment as (
    select member_id
    , title 
    from(
        select * 
        from {{source('postgres_public', 'employment_histories')}}
        where
        id in (
            select
                FIRST_VALUE(id) over(partition by member_id order by created_at desc rows between unbounded preceding and unbounded following)
            from {{source('postgres_public', 'employment_histories')}} 
            where not _fivetran_deleted
        )
    )
)

, user_info as (    
    select
        u.id as user_id
        , coalesce(cc.alpha_two_letter, ui.country_code) as country_code
        , ui.first_name
        , ui.last_name
        , ui.phone_number
        , ui.state_code
        , ui.marketing_consented_at is not null as marketing_consent
    from {{source('postgres_public', 'users')}} u
    left join {{source('postgres_public', 'user_infos')}} ui on not ui._fivetran_deleted and u.id = ui.user_id
    left join workshop_public.country_codes cc on len(ui.country_code)=3 and ui.country_code = cc.alpha_three_letter
)

, recent_member as (
    select u.id as user_id 
    , coalesce(am.first_name, cm.first_name) as first_name
    , coalesce(am.last_name, cm.last_name) as last_name
    , coalesce(am.personal_mobile_number, cm.personal_mobile_number) as personal_mobile
    , coalesce(am.date_of_birth, cm.date_of_birth) as date_of_birth
    , case when not a._fivetran_deleted then a.state else null end as state
    , case when not a._fivetran_deleted and a.state ~* '^[\\W]?nsw|act|^[\\W]?vic|^nt|qld|^s[.]?a[.]?$|tas|new south wales|sydney|^w[.]?a$|western aus|south australia|queensland|northern territory|australia' then 'AU'
        else coalesce(cc3.alpha_two_letter, ccx.alpha_two_letter, case when len(a.country)<2 or len(a.country) > 3 then null else a.country end) end as country
    , coalesce(case when not loc._fivetran_deleted then loc.country else null end, am.work_country) as working_country 
    , case when coalesce(am.role, cm.role)~ 'employer' then 'admin' else coalesce(am.role, cm.role) end as role
    , ce.title 
    , o.name as company
    , coalesce(am.organisation_id, cm.organisation_id) as organisation_id
    , ic.title as industry
    , o.setup_mode
    , trim('Auth' from epa.type) as payroll_type
    , json_extract_path_text(epa.data, 'kp_white_label') as white_label
    , epa.connected_app
    , o.sub_name
    , o.business_account_id
    , ba.name as business_account_name
    , case when am.id is not null then null else t.termination_date end as termination_date
    , oe.active_employees
    , coalesce(am.independent_contractor, cm.independent_contractor) as independent_contractor
    from postgres_public.users u   
    -- last active member         
    left join (
        select *      
        from {{ source('postgres_public', 'members') }}
        where
        id in (
            select
                FIRST_VALUE(m.id) over(partition by m.user_id order by m.created_at desc rows between unbounded preceding and unbounded following)
            from {{ source('postgres_public', 'members') }} m
            join {{ref('employment_hero_organisations')}} o on m.organisation_id = o.id 
            where 
            not m._fivetran_deleted 
            and not m.system_manager 
            and not m.system_user 
            and not m.is_shadow_data
            and active            
            and o.sub_name not ilike '%demo%'
            and (m.created_at < m.termination_date or m.termination_date is null)
        )
    )am on am.user_id = u.id
    -- last created member
    left join (
        select *      
        from {{ source('postgres_public', 'members') }}
        where
            id in (
            select
                FIRST_VALUE(m.id) over(partition by m.user_id order by m.created_at desc rows between unbounded preceding and unbounded following)
            from {{ source('postgres_public', 'members') }} m
            join {{ref('employment_hero_organisations')}} o on m.organisation_id = o.id 
            where 
            not m._fivetran_deleted 
            and not m.system_manager 
            and not m.system_user 
            and not m.is_shadow_data
            and o.sub_name not ilike '%demo%'
            and (m.created_at < m.termination_date or m.termination_date is null)
            )
    )cm on cm.user_id = u.id
    left join (
        select user_id
        , max(termination_date) as termination_date
        from {{ source('postgres_public', 'members') }} m
        join {{ref('employment_hero_organisations')}} o on m.organisation_id = o.id 
        where not m._fivetran_deleted
        and not m.system_manager 
        and not m.system_user 
        and not m.is_shadow_data
        and o.sub_name not ilike '%demo%'
        group by 1    
    )t on t.user_id = u.id 
    left join {{ source('postgres_public', 'addresses') }} a 
        on a.id = coalesce(am.address_id, cm.address_id) and not a._fivetran_deleted 
    left join workshop_public.country_codes cc3 on len(a.country)=3 and upper(a.country) = cc3.alpha_three_letter
    left join workshop_public.country_codes ccx on len(a.country)>3 and upper(a.country) = upper(ccx.country)
    left join postgres_public.work_locations as loc     
        on loc.id =  coalesce(am.work_location_id, cm.work_location_id) and not loc._fivetran_deleted
    left join current_employment ce on ce.member_id = coalesce(am.id, cm.id) 
    left join {{ref('employment_hero_organisations')}} o on coalesce(am.organisation_id, cm.organisation_id) = o.id
    left join {{source('postgres_public', 'business_accounts')}} ba on o.business_account_id = ba.id and not ba._fivetran_deleted
    left join postgres_public.industry_categories as ic on o.industry_category_id = ic.id and not ic._fivetran_deleted
    left join {{ref('employment_hero_v_last_connected_payroll')}} epa on o.id = epa.organisation_id    
    left join {{ref('employment_hero_v_active_employees_by_organisations')}} as oe on oe.organisation_id = o.id 
    where not u._fivetran_deleted
)

, first_mobile_app_date as (  
  select u.id as user_id, min(timestamp) as first_mobile_app_date
  from {{ ref('customers_events') }} e
  join {{ source('postgres_public', 'users') }} u on u.uuid = e.user_id
  where app_version_string is not null
  group by 1
  )

, first_sign_in as (
    select user_id, min(first_sign_in_at) as first_sign_in_at
    from postgres_public.members m
    where not _fivetran_deleted 
    and not m.system_manager 
    and not m.system_user
     and not m.is_shadow_data
    group by 1
)  

, last_log_in_date as (  
  select u.id as user_id, max(timestamp)::date as last_log_in_date
  from {{ ref('customers_events') }} e 
  join {{ source('postgres_public', 'users') }} u on u.uuid = e.user_id    
  group by 1
  )

, managers as (
    select distinct m.user_id    
    from
        postgres_public.member_managers mm 
        join postgres_public.members m on m.id = mm.manager_id        
        join  {{ref('employment_hero_organisations')}} o on m.organisation_id = o.id
        where not m.system_manager 
        and not m.system_user 
        and not m.is_shadow_data
        and not mm._fivetran_deleted 
        and not m._fivetran_deleted 
        and not o._fivetran_deleted
        and o.sub_name not ilike '%demo%'
        and (m.created_at < m.termination_date or m.termination_date is null) 
)  

, ei_users as (
    select distinct m.user_id
    from postgres_public.members m 
    join {{ref('employment_hero_v_ei_organisations')}} o on m.organisation_id = o.organisation_id 
    where not m._fivetran_deleted 
    and m.active 
    and not m.system_manager 
    and not m.system_user 
    and not m.is_shadow_data    
    and (m.created_at < m.termination_date or m.termination_date is null)  
)

, career_enabled_users as (
    select m.user_id
    , case when sum(case when bo.organisation_id is null then 1 else 0 end) > 0 then true else false end as career_enabled    
    from postgres_public.members m 
    join  {{ref('employment_hero_organisations')}} o on m.organisation_id = o.id 
    left join {{ref('employment_hero_v_swag_career_blacklist_organisations')}} bo on m.organisation_id = bo.organisation_id 
    where not m.system_manager 
    and not m.system_user 
    and not m.is_shadow_data
    and not m._fivetran_deleted 
    and not o._fivetran_deleted
    and m.active
    group by 1
)

, instapay_enabled_users as (
    select m.user_id
    , case when sum(case when instapay_enabled then 1 else 0 end) > 0 then true
        when sum(case when not instapay_enabled then 1 else 0 end) > 0 then false
        end as instapay_enabled    
    from postgres_public.members m 
    join  {{ref('employment_hero_organisations')}} o on m.organisation_id = o.id 
    left join {{ref('ebenefits_v_instapay_on_off_organisations')}} io on m.organisation_id = io.organisation_id 
    where not m.system_manager 
    and not m.system_user 
    and not m.is_shadow_data
    and not m._fivetran_deleted
    and not o._fivetran_deleted
    and m.active
    group by 1
)

select u.id as eh_platform_user_id
, coalesce(m.first_name, ui.first_name) as first_name
, coalesce(m.last_name, ui.last_name) as last_name
, u.email
, coalesce(m.personal_mobile, ui.phone_number) as personal_mobile
, m.date_of_birth
, coalesce(m.state, ui.state_code) as state
, coalesce(ui.country_code, m.country) as country
, m.working_country as eh_Platform_Employment_Location
, m.role as EH_Platform_Role__c
, u.created_at::date as eh_Platform_Creation_Date
, fs.first_sign_in_at::date as eh_Platform_Join_Date
, ma.first_mobile_app_date::date as eh_Platform_First_Mobile_Access_Date
, m.title
, m.organisation_id as Org_ID__c
, m.company
, m.industry as eh_Platform_Industry
, m.setup_mode as eh_PLatform_SetUp_Mode
, m.payroll_type as EH_Platform_Connected_Payroll__c
, m.connected_app as eh_Platform_Branded_Payroll
, m.sub_name as EH_Platform_Subscription_Level__c
, greatest(u.last_sign_in_at::date, ll.last_log_in_date) as eh_Platform_Last_LogIn_Date
, son.store_enabled as eh_Platform_swag_store_enabled
, iu.instapay_enabled as eh_Platform_org_Instapay_Enabled
, m.termination_date as Termination_Date__c
, case when man.user_id is not null then true else false end as EH_Platform_Manager__c
, case when eiu.user_id is not null then true else false end as Managed_by_EI__c
, m.white_label as eh_Platform_White_labelled_Payroll
--, m.active_employees as Number_Of_Employees
, mon.money_enabled as eh_Platform_Money_Enabled
, case when ceu.career_enabled is null then true else ceu.career_enabled end as eh_Platform_Career_Enabled
, bon.benefits_enabled
, m.independent_contractor as eh_platform_contractor
, m.business_account_name as eh_platform_bussiness_portal_account
, ui.marketing_consent as eh_platform_marketing_consent
from postgres_public.users u
left join recent_member m on m.user_id = u.id
left join user_info ui on u.id = ui.user_id
left join first_sign_in as fs on fs.user_id = u.id 
left join first_mobile_app_date ma on ma.user_id = u.id 
left join last_log_in_date ll on ll.user_id = u.id 
left join managers man on man.user_id = u.id 
left join ei_users eiu on eiu.user_id = u.id 
left join {{ref('ebenefits_v_money_pillar_on_off_users')}} mon on mon.user_id = u.id 
left join {{ref('ebenefits_v_benefits_pillar_on_off_users')}} bon on bon.user_id = u.id
left join {{ref('ebenefits_v_swag_store_on_off_users')}} son on son.user_id = u.id
left join instapay_enabled_users iu on iu.user_id = u.id 
left join career_enabled_users ceu on ceu.user_id = u.id 
left join {{ref('ats_candidate_profiles')}} can on can.id = u.id
where {{legit_emails('u.email')}} 
and not u.is_shadow_data
and not u._fivetran_deleted
and (m.organisation_id is not null or can.id is not null)

{% endsnapshot %}