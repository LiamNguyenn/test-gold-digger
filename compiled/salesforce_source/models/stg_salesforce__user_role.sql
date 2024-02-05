--To disable this model, set the using_user_role variable within your dbt_project.yml file to False.


with base as (

    select *
    from "dev"."salesforce"."stg_salesforce__user_role_tmp"
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
    
    
    developer_name
    
 as 
    
    developer_name
    
, 
    
    
    id
    
 as 
    
    id
    
, 
    
    
    name
    
 as 
    
    name
    
, 
    
    
    opportunity_access_for_account_owner
    
 as 
    
    opportunity_access_for_account_owner
    
, 
    
    
    parent_role_id
    
 as 
    
    parent_role_id
    
, 
    
    
    rollup_description
    
 as 
    
    rollup_description
    




    from base
), 

final as (

    select
        _fivetran_deleted,
        cast(_fivetran_synced as timestamp) as _fivetran_synced,
        developer_name,
        id as user_role_id,
        name as user_role_name,
        opportunity_access_for_account_owner,
        parent_role_id,
        rollup_description
        
        




        
    from fields
)

select *
from final