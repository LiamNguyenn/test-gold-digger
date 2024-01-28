{{
    config(
        materialized='incremental',
        alias='onboard_candidate_event'
    )
}}

SELECT
  e._fivetran_id as message_id
  , e.time
  , e.name
  , split_part(current_url,'candidate_id=',2) as candidate_job_id
  , u.uuid as user_id 
  , case when json_extract_path_text(properties, 'module') != '' and e.name != 'Visit Company Feed' then json_extract_path_text(properties, 'module')
  when app_version_string is not null then 'mobile' 
  else 'others' end as module
  , json_extract_path_text(properties, 'sub_module') as sub_module
  , regexp_substr(name, '[^#]*$') as mobile_page
  , (case when json_extract_path_text(properties, 'organisation_id')= '' then null else json_extract_path_text(properties, 'organisation_id') end)::int as organisation_id
  , m.id as member_id 
  , json_extract_path_text(properties, 'user_type') as user_type
  , json_extract_path_text(properties, 'platform') as platform   
  , os
  , device  
  , browser
  , screen_width
  , screen_height
  , screen_dpi
  , app_version_string
from
  {{ source('mp', 'event') }} e  
  join {{ source('postgres_public', 'members') }} m on json_extract_path_text(properties, 'member_id') = m.id
  join {{ source('postgres_public', 'users') }} u on coalesce(e.user_id, json_extract_path_text(properties, 'user_uuid', true)) = u.uuid
where
    e.name ~ 'Onboard Candidate'
    and candidate_job_id != ''
    and (e.user_id is not null or json_extract_path_text(properties, 'user_uuid') != '')
    and not m._fivetran_deleted
    and not m.is_shadow_data
    and not u._fivetran_deleted
    and not u.is_shadow_data
    and u.email !~* '.*(employmenthero|employmentinnovations|thinkei|p2m|power2motivate|test|demo|exacc|sandbox|\\+).*'
    and not m.system_manager
    and not m.system_user
{% if is_incremental() %}
    and e.time > (SELECT MAX(time) FROM {{ this }} ) 
{% endif %}