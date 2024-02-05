--To disable this model, set the using_user_role variable within your dbt_project.yml file to False.


with base as (

    select * 
    from "dev"."salesforce"."stg_salesforce__event_tmp"
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
    
    
    activity_date_time
    
 as 
    
    activity_date_time
    
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
    
    
    end_date
    
 as 
    
    end_date
    
, 
    
    
    end_date_time
    
 as 
    
    end_date_time
    
, 
    
    
    event_subtype
    
 as 
    
    event_subtype
    
, 
    
    
    group_event_type
    
 as 
    
    group_event_type
    
, 
    
    
    id
    
 as 
    
    id
    
, 
    
    
    is_archived
    
 as 
    
    is_archived
    
, 
    
    
    is_child
    
 as 
    
    is_child
    
, 
    
    
    is_deleted
    
 as 
    
    is_deleted
    
, 
    
    
    is_group_event
    
 as 
    
    is_group_event
    
, 
    
    
    is_recurrence
    
 as 
    
    is_recurrence
    
, 
    
    
    last_modified_by_id
    
 as 
    
    last_modified_by_id
    
, 
    
    
    last_modified_date
    
 as 
    
    last_modified_date
    
, 
    
    
    location
    
 as 
    
    location
    
, 
    
    
    owner_id
    
 as 
    
    owner_id
    
, 
    
    
    start_date_time
    
 as 
    
    start_date_time
    
, 
    
    
    subject
    
 as 
    
    subject
    
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
        id as event_id,
        account_id,
        cast(activity_date as timestamp) as activity_date,
        cast(activity_date_time as timestamp) as activity_date_time,
        created_by_id,
        cast(created_date as timestamp) as created_date,
        description as event_description,
        cast(end_date as timestamp) as end_date,
        cast(end_date_time as timestamp) as end_date_time,
        event_subtype,
        group_event_type,
        is_archived,
        is_child,
        is_deleted,
        is_group_event,
        is_recurrence,
        last_modified_by_id,
        cast(last_modified_date as timestamp) as last_modified_date,
        location,
        owner_id,
        cast(start_date_time as timestamp) as start_date_time,
        subject,
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