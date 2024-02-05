--To disable this model, set the using_schedules variable within your dbt_project.yml file to False.


with base as (

    select * 
    from "dev"."zendesk"."stg_zendesk__daylight_time_tmp"

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
    
    
    daylight_end_utc
    
 as 
    
    daylight_end_utc
    
, 
    
    
    daylight_offset
    
 as 
    
    daylight_offset
    
, 
    
    
    daylight_start_utc
    
 as 
    
    daylight_start_utc
    
, 
    
    
    time_zone
    
 as 
    
    time_zone
    
, 
    
    
    year
    
 as 
    
    year
    



        
    from base
),

final as (
    
    select 
        daylight_end_utc,
        daylight_offset,
        daylight_start_utc,
        time_zone,
        year,
        daylight_offset * 60 as daylight_offset_minutes
        
    from fields
)

select * from final