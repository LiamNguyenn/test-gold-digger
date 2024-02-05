with base as (

    select * 
    from "dev"."zendesk"."stg_zendesk__ticket_tmp"

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
    
    
    allow_channelback
    
 as 
    
    allow_channelback
    
, 
    
    
    assignee_id
    
 as 
    
    assignee_id
    
, 
    
    
    brand_id
    
 as 
    
    brand_id
    
, 
    
    
    created_at
    
 as 
    
    created_at
    
, 
    
    
    description
    
 as 
    
    description
    
, 
    
    
    due_at
    
 as 
    
    due_at
    
, 
    
    
    external_id
    
 as 
    
    external_id
    
, 
    
    
    forum_topic_id
    
 as 
    
    forum_topic_id
    
, 
    
    
    group_id
    
 as 
    
    group_id
    
, 
    
    
    has_incidents
    
 as 
    
    has_incidents
    
, 
    
    
    id
    
 as 
    
    id
    
, 
    
    
    is_public
    
 as 
    
    is_public
    
, 
    
    
    merged_ticket_ids
    
 as 
    
    merged_ticket_ids
    
, 
    
    
    organization_id
    
 as 
    
    organization_id
    
, 
    
    
    priority
    
 as 
    
    priority
    
, 
    
    
    problem_id
    
 as 
    
    problem_id
    
, 
    
    
    recipient
    
 as 
    
    recipient
    
, 
    
    
    requester_id
    
 as 
    
    requester_id
    
, 
    
    
    status
    
 as 
    
    status
    
, 
    
    
    subject
    
 as 
    
    subject
    
, 
    
    
    submitter_id
    
 as 
    
    submitter_id
    
, 
    
    
    system_ccs
    
 as 
    
    system_ccs
    
, 
    
    
    system_client
    
 as 
    
    system_client
    
, 
    
    
    system_ip_address
    
 as 
    
    system_ip_address
    
, 
    
    
    system_json_email_identifier
    
 as 
    
    system_json_email_identifier
    
, 
    
    
    system_latitude
    
 as 
    
    system_latitude
    
, 
    
    
    system_location
    
 as 
    
    system_location
    
, 
    
    
    system_longitude
    
 as 
    
    system_longitude
    
, 
    
    
    system_machine_generated
    
 as 
    
    system_machine_generated
    
, 
    
    
    system_message_id
    
 as 
    
    system_message_id
    
, 
    
    
    system_raw_email_identifier
    
 as 
    
    system_raw_email_identifier
    
, 
    
    
    ticket_form_id
    
 as 
    
    ticket_form_id
    
, 
    
    
    type
    
 as 
    
    type
    
, 
    
    
    updated_at
    
 as 
    
    updated_at
    
, 
    
    
    url
    
 as 
    
    url
    
, 
    
    
    via_channel
    
 as 
    
    via_channel
    
, 
    
    
    via_source_from_address
    
 as 
    
    via_source_from_address
    
, 
    
    
    via_source_from_id
    
 as 
    
    via_source_from_id
    
, 
    
    
    via_source_from_title
    
 as 
    
    via_source_from_title
    
, 
    
    
    via_source_rel
    
 as 
    
    via_source_rel
    
, 
    
    
    via_source_to_address
    
 as 
    
    via_source_to_address
    
, 
    
    
    via_source_to_name
    
 as 
    
    via_source_to_name
    




        --The below script allows for pass through columns.
        
        
    from base
),

final as (
    
    select 
        id as ticket_id,
        _fivetran_synced,
        assignee_id,
        brand_id,
        cast(created_at as timestamp without time zone) as created_at,
            cast(updated_at as timestamp without time zone) as updated_at,
        
        description,
        due_at,
        group_id,
        external_id,
        is_public,
        organization_id,
        priority,
        recipient,
        requester_id,
        status,
        subject,
        problem_id,
        submitter_id,
        ticket_form_id,
        type,
        url,
        via_channel as created_channel,
        via_source_from_id as source_from_id,
        via_source_from_title as source_from_title,
        via_source_rel as source_rel,
        via_source_to_address as source_to_address,
        via_source_to_name as source_to_name

        --The below script allows for pass through columns.
        

    from fields
)

select * 
from final