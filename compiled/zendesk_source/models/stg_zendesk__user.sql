with base as (

    select * 
    from "dev"."zendesk"."stg_zendesk__user_tmp"

),

fields as (

    select
        /*
        The below macro is used to generate the correct SQL for package staging models. It takes a list of columns 
        that are expected/needed (staging_columns from dbt_zendesk_source/models/tmp/) and compares it with columns 
        in the source (source_columns from dbt_zendesk_source/macros/).
        For more information refer to our dbt_fivetran_utils documentation (https://github.com/fivetran/dbt_fivetran_utils.git).
        */
        
    
    
    _fivetran_synced
    
 as 
    
    _fivetran_synced
    
, 
    
    
    active
    
 as 
    
    active
    
, 
    
    
    alias
    
 as 
    
    alias
    
, 
    
    
    authenticity_token
    
 as 
    
    authenticity_token
    
, 
    
    
    chat_only
    
 as 
    
    chat_only
    
, 
    
    
    created_at
    
 as 
    
    created_at
    
, 
    
    
    details
    
 as 
    
    details
    
, 
    
    
    email
    
 as 
    
    email
    
, 
    
    
    external_id
    
 as 
    
    external_id
    
, 
    
    
    id
    
 as 
    
    id
    
, 
    
    
    last_login_at
    
 as 
    
    last_login_at
    
, 
    
    
    locale
    
 as 
    
    locale
    
, 
    
    
    locale_id
    
 as 
    
    locale_id
    
, 
    
    
    moderator
    
 as 
    
    moderator
    
, 
    
    
    name
    
 as 
    
    name
    
, 
    
    
    notes
    
 as 
    
    notes
    
, 
    
    
    only_private_comments
    
 as 
    
    only_private_comments
    
, 
    
    
    organization_id
    
 as 
    
    organization_id
    
, 
    
    
    phone
    
 as 
    
    phone
    
, 
    
    
    remote_photo_url
    
 as 
    
    remote_photo_url
    
, 
    
    
    restricted_agent
    
 as 
    
    restricted_agent
    
, 
    
    
    role
    
 as 
    
    role
    
, 
    
    
    shared
    
 as 
    
    shared
    
, 
    
    
    shared_agent
    
 as 
    
    shared_agent
    
, 
    
    
    signature
    
 as 
    
    signature
    
, 
    
    
    suspended
    
 as 
    
    suspended
    
, 
    
    
    ticket_restriction
    
 as 
    
    ticket_restriction
    
, 
    
    
    time_zone
    
 as 
    
    time_zone
    
, 
    
    
    two_factor_auth_enabled
    
 as 
    
    two_factor_auth_enabled
    
, 
    
    
    updated_at
    
 as 
    
    updated_at
    
, 
    
    
    url
    
 as 
    
    url
    
, 
    
    
    verified
    
 as 
    
    verified
    



        
    from base
),

final as ( 
    
    select 
        id as user_id,
        external_id,
        _fivetran_synced,
        cast(last_login_at as timestamp without time zone) as last_login_at,
            cast(created_at as timestamp without time zone) as created_at,
            cast(updated_at as timestamp without time zone) as updated_at,
        email,
        name,
        organization_id,
        role,
        ticket_restriction,
        time_zone,
        locale,
        active as is_active,
        suspended as is_suspended
    from fields
)

select * 
from final