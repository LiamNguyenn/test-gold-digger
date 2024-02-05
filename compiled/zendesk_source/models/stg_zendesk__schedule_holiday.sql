--To disable this model, set the using_schedules variable within your dbt_project.yml file to False.


with base as (

    select * 
    from "dev"."zendesk"."stg_zendesk__schedule_holiday_tmp"
),

fields as (

    select
        
    
    
    _fivetran_deleted
    
 as 
    
    _fivetran_deleted
    
, 
    
    
    _fivetran_synced
    
 as 
    
    _fivetran_synced
    
, 
    cast(null as TEXT) as 
    
    end_date
    
 , 
    
    
    id
    
 as 
    
    id
    
, 
    cast(null as TEXT) as 
    
    name
    
 , 
    
    
    schedule_id
    
 as 
    
    schedule_id
    
, 
    cast(null as TEXT) as 
    
    start_date
    
 


    from base
),

final as (
    
    select
        _fivetran_deleted,
        cast(_fivetran_synced as timestamp ) as _fivetran_synced,
        cast(end_date as timestamp ) as holiday_end_date_at,
        cast(id as TEXT ) as holiday_id,
        name as holiday_name,
        cast(schedule_id as TEXT ) as schedule_id,
        cast(start_date as timestamp ) as holiday_start_date_at
    from fields
)

select *
from final