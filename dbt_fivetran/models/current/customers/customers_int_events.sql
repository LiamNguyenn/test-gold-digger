{{
    config(
        materialized='incremental',
        alias='int_events',
        on_schema_change='append_new_columns'
    )
}}

with
  all_events as (
    select
        e._fivetran_id as message_id
        , e.time as timestamp
        , e.name
        , os
        , device  
        , browser
        , screen_width
        , screen_height
        , screen_dpi
        , app_version_string
        , json_extract_path_text(properties, 'module') as module
        , json_extract_path_text(properties, 'sub_module') as sub_module
        , coalesce(
            (case when user_id = '' then null else user_id end), 
            json_extract_path_text(properties, 'user_id')
          ) as user_id
        , json_extract_path_text(properties, 'user_uuid') as user_uuid
        , json_extract_path_text(properties, 'eh_user_type') as eh_user_type
        , json_extract_path_text(properties, 'login_provider') as login_provider
        , json_extract_path_text(properties, 'email') as email
        , json_extract_path_text(properties, 'member_id') as member_id
        , json_extract_path_text(properties, 'member_uuid') as member_uuid
        , json_extract_path_text(properties, 'organisation_id') as organisation_id
        , json_extract_path_text(properties, 'user_type') as user_type
        , json_extract_path_text(properties, 'user_email') as user_email
        , json_extract_path_text(properties, 'kp_employee_id') as kp_employee_id
        , json_extract_path_text(properties, 'kp_business_id') as kp_business_id
        , json_extract_path_text(properties, 'kp_user_type') as kp_user_type
        , json_extract_path_text(properties, 'platform') as platform
        , json_extract_path_text(properties, 'module')    as shopnow_offer_module
        , json_extract_path_text(properties, 'offerType') as shopnow_offer_type
        , json_extract_path_text(properties, 'category')  as shopnow_offer_category
        , json_extract_path_text(properties, 'utm_source') as utm_source
        , json_extract_path_text(properties, 'utm_medium') as utm_medium
        , json_extract_path_text(properties, 'utm_campaign') as utm_campaign
        , json_extract_path_text(properties, 'utm_content') as utm_content
        , json_extract_path_text(properties, 'utm_term') as utm_term
    from
        {{ source('mp', 'event') }} e
    where        
        e.name not like '%$%'        
        -- For non prod environments, only pull the last 2 days of data.
        {% if target.name != 'prod' -%}
            AND e.time >= dateadd(day, -2, CURRENT_DATE)
        {%- endif %}

  )

select 
* 
from all_events e
{% if is_incremental() %}       
     where e.timestamp > (SELECT MAX(ie.timestamp) FROM {{ this }} ie)
{% endif %}
