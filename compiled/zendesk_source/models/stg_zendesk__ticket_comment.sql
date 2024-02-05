with base as (

    select * 
    from "dev"."zendesk"."stg_zendesk__ticket_comment_tmp"

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
    
    
    body
    
 as 
    
    body
    
, 
    
    
    call_duration
    
 as 
    
    call_duration
    
, 
    
    
    call_id
    
 as 
    
    call_id
    
, 
    
    
    created
    
 as 
    
    created
    
, 
    
    
    facebook_comment
    
 as 
    
    facebook_comment
    
, 
    
    
    id
    
 as 
    
    id
    
, 
    
    
    location
    
 as 
    
    location
    
, 
    
    
    public
    
 as 
    
    public
    
, 
    
    
    recording_url
    
 as 
    
    recording_url
    
, 
    
    
    started_at
    
 as 
    
    started_at
    
, 
    
    
    ticket_id
    
 as 
    
    ticket_id
    
, 
    
    
    transcription_status
    
 as 
    
    transcription_status
    
, 
    
    
    transcription_text
    
 as 
    
    transcription_text
    
, 
    
    
    trusted
    
 as 
    
    trusted
    
, 
    
    
    tweet
    
 as 
    
    tweet
    
, 
    
    
    user_id
    
 as 
    
    user_id
    
, 
    
    
    voice_comment
    
 as 
    
    voice_comment
    
, 
    
    
    voice_comment_transcription_visible
    
 as 
    
    voice_comment_transcription_visible
    



        
    from base
),

final as (
    
    select 
        id as ticket_comment_id,
        _fivetran_synced,
        body,
        cast(created as timestamp without time zone) as created_at,
        
        public as is_public,
        ticket_id,
        user_id,
        facebook_comment as is_facebook_comment,
        tweet as is_tweet,
        voice_comment as is_voice_comment
    from fields
)

select * 
from final