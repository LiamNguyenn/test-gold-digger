--To disable this model, set the using_user_role variable within your dbt_project.yml file to False.


with base as (

    select * 
    from "dev"."salesforce"."stg_salesforce__opportunity_line_item_tmp"
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
    
    
    discount
    
 as 
    
    discount
    
, 
    cast(null as boolean) as 
    
    has_quantity_schedule
    
 , 
    cast(null as boolean) as 
    
    has_revenue_schedule
    
 , 
    cast(null as boolean) as 
    
    has_schedule
    
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
    
    
    list_price
    
 as 
    
    list_price
    
, 
    
    
    name
    
 as 
    
    name
    
, 
    
    
    opportunity_id
    
 as 
    
    opportunity_id
    
, 
    
    
    pricebook_entry_id
    
 as 
    
    pricebook_entry_id
    
, 
    
    
    product_2_id
    
 as 
    
    product_2_id
    
, 
    
    
    product_code
    
 as 
    
    product_code
    
, 
    
    
    quantity
    
 as 
    
    quantity
    
, 
    
    
    service_date
    
 as 
    
    service_date
    
, 
    
    
    sort_order
    
 as 
    
    sort_order
    
, 
    
    
    total_price
    
 as 
    
    total_price
    
, 
    
    
    unit_price
    
 as 
    
    unit_price
    



        
    from base
), 

final as (
    
    select 
        cast(_fivetran_synced as timestamp) as _fivetran_synced,
        id as opportunity_line_item_id,
        created_by_id,
        cast(created_date as timestamp) as created_date,
        description as opportunity_line_item_description,
        discount,
        has_quantity_schedule,
        has_revenue_schedule,
        has_schedule,
        is_deleted,
        last_modified_by_id,
        cast(last_modified_date as timestamp) as last_modified_date,
        cast(last_referenced_date as timestamp) as last_referenced_date,
        cast(last_viewed_date as timestamp) as last_viewed_date,
        list_price,
        name as opportunity_line_item_name,
        opportunity_id,
        pricebook_entry_id,
        product_2_id,
        product_code,
        quantity,
        cast(service_date as timestamp) as service_date,
        sort_order,
        total_price,
        unit_price
        
        




        
    from fields
)

select *
from final
where not coalesce(is_deleted, false)