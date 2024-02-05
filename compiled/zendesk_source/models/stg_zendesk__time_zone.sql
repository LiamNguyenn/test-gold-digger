--To disable this model, set the using_schedules variable within your dbt_project.yml file to False.


with base as (

    select * 
    from "dev"."zendesk"."stg_zendesk__time_zone_tmp"

),

fields as (

    select
        
    
    
    _fivetran_synced
    
 as 
    
    _fivetran_synced
    
, 
    
    
    standard_offset
    
 as 
    
    standard_offset
    
, 
    
    
    time_zone
    
 as 
    
    time_zone
    



        
    from base
),

final as (
    
    select 
        standard_offset,
        time_zone,
        -- the standard_offset is a string written as [+/-]HH:MM
        -- let's convert it to an integer value of minutes
        cast( 

  
    

    split_part(
        standard_offset,
        ':',
        1
        )


  

 as integer ) * 60 +
            (cast( 

  
    

    split_part(
        standard_offset,
        ':',
        2
        )


  

 as integer ) *
                (case when standard_offset like '-%' then -1 else 1 end) ) as standard_offset_minutes
    
    from fields
)

select * from final