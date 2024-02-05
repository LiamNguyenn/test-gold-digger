--To disable this model, set the using_user_role variable within your dbt_project.yml file to False.


with base as (

    select * 
    from "dev"."salesforce"."stg_salesforce__product_2_tmp"
),

fields as (

    select
        
    
    
    _fivetran_synced
    
 as 
    
    _fivetran_synced
    
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
    
    
    display_url
    
 as 
    
    display_url
    
, 
    
    
    external_id
    
 as 
    
    external_id
    
, 
    
    
    family
    
 as 
    
    family
    
, 
    
    
    id
    
 as 
    
    id
    
, 
    
    
    is_active
    
 as 
    
    is_active
    
, 
    
    
    is_archived
    
 as 
    
    is_archived
    
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
    
    
    name
    
 as 
    
    name
    
, 
    cast(null as integer) as 
    
    number_of_quantity_installments
    
 , 
    cast(null as integer) as 
    
    number_of_revenue_installments
    
 , 
    
    
    product_code
    
 as 
    
    product_code
    
, 
    cast(null as TEXT) as 
    
    quantity_installment_period
    
 , 
    cast(null as TEXT) as 
    
    quantity_schedule_type
    
 , 
    
    
    quantity_unit_of_measure
    
 as 
    
    quantity_unit_of_measure
    
, 
    cast(null as TEXT) as 
    
    record_type_id
    
 , 
    cast(null as TEXT) as 
    
    revenue_installment_period
    
 , 
    cast(null as TEXT) as 
    
    revenue_schedule_type
    
 


        
    from base
), 

final as (
    
    select 
        cast(_fivetran_synced as timestamp) as _fivetran_synced,
        id as product_2_id,
        created_by_id,
        cast(created_date as timestamp) as created_date,
        description as product_2_description,
        display_url,
        external_id,
        family,
        is_active,
        is_archived,
        is_deleted,
        last_modified_by_id,
        cast(last_modified_date as timestamp) as last_modified_date,
        cast(last_referenced_date as timestamp) as last_referenced_date,
        cast(last_viewed_date as timestamp) as last_viewed_date,
        name as product_2_name,
        number_of_quantity_installments,
        number_of_revenue_installments,
        product_code,
        quantity_installment_period,
        quantity_schedule_type,
        quantity_unit_of_measure,
        record_type_id,
        revenue_installment_period,
        revenue_schedule_type
        
        




        
    from fields
)

select *
from final
where not coalesce(is_deleted, false)