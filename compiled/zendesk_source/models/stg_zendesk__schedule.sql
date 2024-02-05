--To disable this model, set the using_schedules variable within your dbt_project.yml file to False.


with base as (

    select * 
    from "dev"."zendesk"."stg_zendesk__schedule_tmp"

),

fields as (

    select
        /*
        The below macro is used to generate the correct SQL for package staging models. It takes a list of columns 
        that are expected/needed (staging_columns from dbt_zendesk_source/models/tmp/) and compares it with columns 
        in the source (source_columns from dbt_zendesk_source/macros/).
        For more information refer to our dbt_fivetran_utils documentation (https://github.com/fivetran/dbt_fivetran_utils.git).
        */
        
    
    
    _fivetran_deleted
    
 as 
    
    _fivetran_deleted
    
, 
    
    
    _fivetran_synced
    
 as 
    
    _fivetran_synced
    
, 
    
    
    created_at
    
 as 
    
    created_at
    
, 
    
    
    end_time
    
 as 
    
    end_time
    
, 
    
    
    end_time_utc
    
 as 
    
    end_time_utc
    
, 
    
    
    id
    
 as 
    
    id
    
, 
    
    
    name
    
 as 
    
    name
    
, 
    
    
    start_time
    
 as 
    
    start_time
    
, 
    
    
    start_time_utc
    
 as 
    
    start_time_utc
    
, 
    
    
    time_zone
    
 as 
    
    time_zone
    



        
    from base
),

final as (
    
    select 
        cast(id as TEXT) as schedule_id, --need to convert from numeric to string for downstream models to work properly
        end_time,
        start_time,
        name as schedule_name,
        created_at,
        time_zone
        
    from fields
    where not coalesce(_fivetran_deleted, false)
)

select * 
from final