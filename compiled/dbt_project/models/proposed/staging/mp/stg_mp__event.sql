



with source as (
    select *

    from "dev"."mp"."event" as mp_event

    where
        mp_event.name not like '%$%'
        -- For non prod environments, only pull the last 2 days of data.
        
),

transformed as (
    select
        _fivetran_id                                                                                                                             as event_id,
        time                                                                                                                                     as event_timestamp,
        name,
        os,
        device,
        browser,
        screen_width,
        screen_height,
        screen_dpi,
        app_version_string,
        



    case when json_extract_path_text(properties, 'module')= '' then null else json_extract_path_text(properties, 'module') end as module
    
    ,

    case when json_extract_path_text(properties, 'sub_module')= '' then null else json_extract_path_text(properties, 'sub_module') end as sub_module
    
    ,

    case when json_extract_path_text(properties, 'user_id')= '' then null else json_extract_path_text(properties, 'user_id') end as user_id
    
    ,

    case when json_extract_path_text(properties, 'user_uuid')= '' then null else json_extract_path_text(properties, 'user_uuid') end as user_uuid
    
    ,

    case when json_extract_path_text(properties, 'eh_user_type')= '' then null else json_extract_path_text(properties, 'eh_user_type') end as eh_user_type
    
    ,

    case when json_extract_path_text(properties, 'login_provider')= '' then null else json_extract_path_text(properties, 'login_provider') end as login_provider
    
    ,

    case when json_extract_path_text(properties, 'email')= '' then null else json_extract_path_text(properties, 'email') end as email
    
    ,

    case when json_extract_path_text(properties, 'member_id')= '' then null else json_extract_path_text(properties, 'member_id') end as eh_employee_id
    
    ,

    case when json_extract_path_text(properties, 'member_uuid')= '' then null else json_extract_path_text(properties, 'member_uuid') end as eh_employee_uuid
    
    ,

    case when json_extract_path_text(properties, 'organisation_id')= '' then null else json_extract_path_text(properties, 'organisation_id') end as eh_organisation_id
    
    ,

    case when json_extract_path_text(properties, 'user_type')= '' then null else json_extract_path_text(properties, 'user_type') end as user_type
    
    ,

    case when json_extract_path_text(properties, 'user_email')= '' then null else json_extract_path_text(properties, 'user_email') end as user_email
    
    ,

    case when json_extract_path_text(properties, 'kp_employee_id')= '' then null else json_extract_path_text(properties, 'kp_employee_id') end as kp_employee_id
    
    ,

    case when json_extract_path_text(properties, 'kp_business_id')= '' then null else json_extract_path_text(properties, 'kp_business_id') end as kp_business_id
    
    ,

    case when json_extract_path_text(properties, 'platform')= '' then null else json_extract_path_text(properties, 'platform') end as platform
    
    ,

    case when json_extract_path_text(properties, 'offerType')= '' then null else json_extract_path_text(properties, 'offerType') end as shopnow_offer_type
    
    ,

    case when json_extract_path_text(properties, 'category')= '' then null else json_extract_path_text(properties, 'category') end as shopnow_offer_category
    
    ,

    case when json_extract_path_text(properties, 'utm_source')= '' then null else json_extract_path_text(properties, 'utm_source') end as utm_source
    
    ,

    case when json_extract_path_text(properties, 'utm_medium')= '' then null else json_extract_path_text(properties, 'utm_medium') end as utm_medium
    
    ,

    case when json_extract_path_text(properties, 'utm_campaign')= '' then null else json_extract_path_text(properties, 'utm_campaign') end as utm_campaign
    
    ,

    case when json_extract_path_text(properties, 'utm_content')= '' then null else json_extract_path_text(properties, 'utm_content') end as utm_content
    
    ,

    case when json_extract_path_text(properties, 'utm_term')= '' then null else json_extract_path_text(properties, 'utm_term') end as utm_term
    
    



    from source
)

select * from transformed