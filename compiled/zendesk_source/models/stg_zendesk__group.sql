with base as (

    select * 
    from "dev"."zendesk"."stg_zendesk__group_tmp"

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
    
    
    id
    
 as 
    
    id
    
, 
    
    
    name
    
 as 
    
    name
    
, 
    
    
    updated_at
    
 as 
    
    updated_at
    
, 
    
    
    url
    
 as 
    
    url
    



        
    from base
),

final as (
    
    select 
        id as group_id,
        name
    from fields
    
    where not coalesce(_fivetran_deleted, false)
)

select * 
from final