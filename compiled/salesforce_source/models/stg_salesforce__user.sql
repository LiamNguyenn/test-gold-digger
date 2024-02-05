with base as (

    select *
    from "dev"."salesforce"."stg_salesforce__user_tmp"
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
    
    
    account_id
    
 as 
    
    account_id
    
, 
    
    
    alias
    
 as 
    
    alias
    
, 
    
    
    city
    
 as 
    
    city
    
, 
    
    
    company_name
    
 as 
    
    company_name
    
, 
    
    
    contact_id
    
 as 
    
    contact_id
    
, 
    
    
    country
    
 as 
    
    country
    
, 
    cast(null as TEXT) as 
    
    country_code
    
 , 
    
    
    department
    
 as 
    
    department
    
, 
    
    
    email
    
 as 
    
    email
    
, 
    
    
    first_name
    
 as 
    
    first_name
    
, 
    
    
    id
    
 as 
    
    id
    
, 
    
    
    individual_id
    
 as 
    
    individual_id
    
, 
    
    
    is_active
    
 as 
    
    is_active
    
, 
    
    
    last_login_date
    
 as 
    
    last_login_date
    
, 
    
    
    last_name
    
 as 
    
    last_name
    
, 
    
    
    last_referenced_date
    
 as 
    
    last_referenced_date
    
, 
    
    
    last_viewed_date
    
 as 
    
    last_viewed_date
    
, 
    
    
    manager_id
    
 as 
    
    manager_id
    
, 
    
    
    name
    
 as 
    
    name
    
, 
    
    
    postal_code
    
 as 
    
    postal_code
    
, 
    
    
    profile_id
    
 as 
    
    profile_id
    
, 
    
    
    state
    
 as 
    
    state
    
, 
    cast(null as TEXT) as 
    
    state_code
    
 , 
    
    
    street
    
 as 
    
    street
    
, 
    
    
    title
    
 as 
    
    title
    
, 
    
    
    user_role_id
    
 as 
    
    user_role_id
    
, 
    
    
    user_type
    
 as 
    
    user_type
    
, 
    
    
    username
    
 as 
    
    username
    
, 
    cast(null as TEXT) as 
    
    market
    
 



    from base
), 

final as (
    
    select 
        _fivetran_deleted,
        cast(_fivetran_synced as timestamp) as _fivetran_synced,
        account_id,
        alias,
        city,
        company_name,
        contact_id,
        country,
        country_code,
        department,
        email,
        first_name,
        id as user_id,
        individual_id,
        is_active,
        cast(last_login_date as timestamp) as last_login_date,
        last_name,
        cast(last_referenced_date as timestamp) as last_referenced_date,
        cast(last_viewed_date as timestamp) as last_viewed_date,
        manager_id,
        name as user_name,
        postal_code,
        profile_id,
        state,
        state_code,
        street,
        title,
        user_role_id,
        user_type,
        username 
        
        


    
        
            
                , market
            
        
    



    
    from fields
)

select * 
from final