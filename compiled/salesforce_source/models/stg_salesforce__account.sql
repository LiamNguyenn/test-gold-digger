with base as (

    select *
    from "dev"."salesforce"."stg_salesforce__account_tmp"
), 

fields as (

    select
    
        
    
    
    _fivetran_synced
    
 as 
    
    _fivetran_synced
    
, 
    
    
    account_number
    
 as 
    
    account_number
    
, 
    
    
    account_source
    
 as 
    
    account_source
    
, 
    
    
    annual_revenue
    
 as 
    
    annual_revenue
    
, 
    
    
    billing_city
    
 as 
    
    billing_city
    
, 
    
    
    billing_country
    
 as 
    
    billing_country
    
, 
    
    
    billing_postal_code
    
 as 
    
    billing_postal_code
    
, 
    
    
    billing_state
    
 as 
    
    billing_state
    
, 
    cast(null as TEXT) as 
    
    billing_state_code
    
 , 
    
    
    billing_street
    
 as 
    
    billing_street
    
, 
    
    
    description
    
 as 
    
    description
    
, 
    
    
    id
    
 as 
    
    id
    
, 
    
    
    industry
    
 as 
    
    industry
    
, 
    
    
    is_deleted
    
 as 
    
    is_deleted
    
, 
    
    
    last_activity_date
    
 as 
    
    last_activity_date
    
, 
    
    
    last_referenced_date
    
 as 
    
    last_referenced_date
    
, 
    
    
    last_viewed_date
    
 as 
    
    last_viewed_date
    
, 
    
    
    master_record_id
    
 as 
    
    master_record_id
    
, 
    
    
    name
    
 as 
    
    name
    
, 
    
    
    number_of_employees
    
 as 
    
    number_of_employees
    
, 
    
    
    owner_id
    
 as 
    
    owner_id
    
, 
    
    
    ownership
    
 as 
    
    ownership
    
, 
    
    
    parent_id
    
 as 
    
    parent_id
    
, 
    cast(null as TEXT) as 
    
    rating
    
 , 
    
    
    record_type_id
    
 as 
    
    record_type_id
    
, 
    
    
    shipping_city
    
 as 
    
    shipping_city
    
, 
    
    
    shipping_country
    
 as 
    
    shipping_country
    
, 
    cast(null as TEXT) as 
    
    shipping_country_code
    
 , 
    
    
    shipping_postal_code
    
 as 
    
    shipping_postal_code
    
, 
    
    
    shipping_state
    
 as 
    
    shipping_state
    
, 
    cast(null as TEXT) as 
    
    shipping_state_code
    
 , 
    
    
    shipping_street
    
 as 
    
    shipping_street
    
, 
    
    
    type
    
 as 
    
    type
    
, 
    
    
    website
    
 as 
    
    website
    
, 
    
    
    geo_code_c
    
 as 
    
    geo_code_c
    




    from base
), 

final as (

    select 
        cast(_fivetran_synced as timestamp) as _fivetran_synced,
        account_number,
        account_source,
        cast(annual_revenue as numeric(28,6)) as annual_revenue,
        billing_city,
        billing_country,
        billing_postal_code,
        billing_state,
        billing_state_code,
        billing_street,
        description as account_description,
        id as account_id,
        industry,
        is_deleted,
        cast(last_activity_date as timestamp) as last_activity_date,
        cast(last_referenced_date as timestamp) as last_referenced_date,
        cast(last_viewed_date as timestamp) as last_viewed_date,
        master_record_id,
        name as account_name,
        number_of_employees,
        owner_id,
        ownership,
        parent_id,
        rating,
        record_type_id,
        shipping_city,
        shipping_country,
        shipping_country_code,
        shipping_postal_code,
        shipping_state,
        shipping_state_code,
        shipping_street,
        type,
        website
        
        


    
        
            
                , geo_code_c
            
        
    



        
    from fields
)

select *
from final
where not coalesce(is_deleted, false)