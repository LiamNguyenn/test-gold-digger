--To disable this model, set the using_schedules variable within your dbt_project.yml file to False.


with base as (

    select * 
    from "dev"."zendesk"."stg_zendesk__ticket_schedule_tmp"

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
    
    
    created_at
    
 as 
    
    created_at
    
, 
    
    
    schedule_id
    
 as 
    
    schedule_id
    
, 
    
    
    ticket_id
    
 as 
    
    ticket_id
    



        
    from base
),

final as (
    
    select 
        ticket_id,
        cast(created_at as timestamp without time zone) as created_at,
        
        cast(schedule_id as TEXT) as schedule_id --need to convert from numeric to string for downstream models to work properly
    from fields
)

select * 
from final