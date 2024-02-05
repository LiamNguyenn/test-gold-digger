--To disable this model, set the using_user_role variable within your dbt_project.yml file to False.


with base as (

    select * 
    from "dev"."salesforce"."stg_salesforce__order_tmp"
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
    
    
    activated_by_id
    
 as 
    
    activated_by_id
    
, 
    
    
    activated_date
    
 as 
    
    activated_date
    
, 
    
    
    billing_city
    
 as 
    
    billing_city
    
, 
    
    
    billing_country
    
 as 
    
    billing_country
    
, 
    cast(null as TEXT) as 
    
    billing_country_code
    
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
    
    
    contract_id
    
 as 
    
    contract_id
    
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
    
    
    id
    
 as 
    
    id
    
, 
    
    
    is_deleted
    
 as 
    
    is_deleted
    
, 
    
    
    last_modified_by_id
    
 as 
    
    last_modified_by_id
    
, 
    
    
    last_modified_date
    
 as 
    
    last_modified_date
    
, 
    
    
    last_referenced_date
    
 as 
    
    last_referenced_date
    
, 
    
    
    last_viewed_date
    
 as 
    
    last_viewed_date
    
, 
    cast(null as TEXT) as 
    
    opportunity_id
    
 , 
    
    
    order_number
    
 as 
    
    order_number
    
, 
    
    
    original_order_id
    
 as 
    
    original_order_id
    
, 
    
    
    owner_id
    
 as 
    
    owner_id
    
, 
    
    
    pricebook_2_id
    
 as 
    
    pricebook_2_id
    
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
    
    
    status
    
 as 
    
    status
    
, 
    
    
    total_amount
    
 as 
    
    total_amount
    
, 
    
    
    type
    
 as 
    
    type
    



        
    from base
), 

final as (
    
    select 
        cast(_fivetran_synced as timestamp) as _fivetran_synced,
        id as order_id,
        account_id,
        activated_by_id,
        cast(activated_date as timestamp) as activated_date,
        billing_city,
        billing_country,
        billing_country_code,
        billing_postal_code,
        billing_state,
        billing_state_code,
        billing_street,
        contract_id,
        created_by_id,
        created_date,
        description as order_description,
        cast(end_date as timestamp) as end_date,
        is_deleted,
        last_modified_by_id,
        cast(last_modified_date as timestamp) as last_modified_date,
        cast(last_referenced_date as timestamp) as last_referenced_date,
        cast(last_viewed_date as timestamp) as last_viewed_date,
        opportunity_id,
        order_number,
        original_order_id,
        owner_id,
        pricebook_2_id,
        shipping_city,
        shipping_country,
        shipping_country_code,
        shipping_postal_code,
        shipping_state,
        shipping_state_code,
        shipping_street,
        status,
        total_amount,
        type
        
        




        
    from fields
)

select *
from final
where not coalesce(is_deleted, false)