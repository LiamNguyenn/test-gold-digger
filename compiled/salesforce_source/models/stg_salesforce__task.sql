--To disable this model, set the using_user_role variable within your dbt_project.yml file to False.


with base as (

    select * 
    from "dev"."salesforce"."stg_salesforce__task_tmp"
),

fields as (

    select
        
    
    
    _fivetran_synced
    
 as 
    
    _fivetran_synced
    
, 
    
    
    account_id
    
 as 
    
    account_id
    
, 
    
    
    activity_date
    
 as 
    
    activity_date
    
, 
    
    
    call_disposition
    
 as 
    
    call_disposition
    
, 
    
    
    call_duration_in_seconds
    
 as 
    
    call_duration_in_seconds
    
, 
    
    
    call_object
    
 as 
    
    call_object
    
, 
    
    
    call_type
    
 as 
    
    call_type
    
, 
    
    
    completed_date_time
    
 as 
    
    completed_date_time
    
, 
    
    
    created_by_id
    
 as 
    
    created_by_id
    
, 
    
    
    created_date
    
 as 
    
    created_date
    
, 
    
    
    description
    
 as 
    
    description
    
, 
    
    
    id
    
 as 
    
    id
    
, 
    
    
    is_archived
    
 as 
    
    is_archived
    
, 
    
    
    is_closed
    
 as 
    
    is_closed
    
, 
    
    
    is_deleted
    
 as 
    
    is_deleted
    
, 
    
    
    is_high_priority
    
 as 
    
    is_high_priority
    
, 
    
    
    last_modified_by_id
    
 as 
    
    last_modified_by_id
    
, 
    
    
    last_modified_date
    
 as 
    
    last_modified_date
    
, 
    
    
    owner_id
    
 as 
    
    owner_id
    
, 
    
    
    priority
    
 as 
    
    priority
    
, 
    
    
    record_type_id
    
 as 
    
    record_type_id
    
, 
    
    
    status
    
 as 
    
    status
    
, 
    
    
    subject
    
 as 
    
    subject
    
, 
    
    
    task_subtype
    
 as 
    
    task_subtype
    
, 
    
    
    type
    
 as 
    
    type
    
, 
    
    
    what_count
    
 as 
    
    what_count
    
, 
    
    
    what_id
    
 as 
    
    what_id
    
, 
    
    
    who_count
    
 as 
    
    who_count
    
, 
    
    
    who_id
    
 as 
    
    who_id
    



        
    from base
), 

final as (
    
    select 
        cast(_fivetran_synced as timestamp) as _fivetran_synced,
        id as task_id,
        account_id,
        cast(activity_date as timestamp) as activity_date,
        call_disposition,
        call_duration_in_seconds,
        call_object,
        call_type,
        cast(completed_date_time as timestamp) as completed_date_time,
        created_by_id,
        cast(created_date as timestamp) as created_date,
        description as task_description,
        is_archived,
        is_closed,
        is_deleted,
        is_high_priority,
        last_modified_by_id,
        cast(last_modified_date as timestamp) as last_modified_date,
        owner_id,
        priority,
        record_type_id,
        status,
        subject,
        task_subtype,
        type,
        what_count,
        what_id,
        who_count,
        who_id
        
        




        
    from fields
)

select *
from final
where not coalesce(is_deleted, false)