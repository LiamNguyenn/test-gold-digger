--This model will only run if you have the underlying opportunity line item table.


with opportunity_line_item as (
    
    select *
    from "dev"."salesforce"."stg_salesforce__opportunity_line_item"
), 

-- If using product_2 table, the following will be included, otherwise it will not.

product_2 as (

select *
from "dev"."salesforce"."stg_salesforce__product_2"
),


final as (

    select
        oli.opportunity_line_item_id,
        oli.opportunity_line_item_name,
        oli.opportunity_line_item_description,
        oli.opportunity_id,
        row_number() over (partition by oli.opportunity_id order by oli.created_date) as line_item_index,
        count(opportunity_line_item_id) over (partition by oli.opportunity_id) as total_line_items,
        oli.created_date,
        oli.last_modified_date,
        oli.service_date,
        oli.pricebook_entry_id,
        oli.product_2_id,
        oli.list_price,
        oli.quantity,
        oli.unit_price,
        oli.total_price,
        oli.has_quantity_schedule,
        oli.has_revenue_schedule

        
        ,
        product_2.product_2_name,
        product_2.product_code,
        product_2.product_2_description,
        product_2.external_id as product_external_id,
        product_2.family as product_family,
        product_2.is_active as product_is_active,
        product_2.is_archived as product_is_archived,
        product_2.is_deleted as product_is_deleted,
        product_2.number_of_quantity_installments as product_number_of_quantity_installments,
        product_2.quantity_installment_period as product_quantity_installment_period,
        product_2.quantity_schedule_type as product_quantity_schedule_type,
        product_2.quantity_unit_of_measure as product_quantity_unit_of_measure,
        product_2.number_of_revenue_installments as product_number_of_revenue_installments,
        product_2.revenue_installment_period as product_revenue_installment_period,
        product_2.revenue_schedule_type as product_revenue_schedule_type
        

        --The below script allows for pass through columns.
        





        
        




        

    from opportunity_line_item as oli

    
    left join product_2
        on oli.product_2_id = product_2.product_2_id
    
)

select * 
from final