with base as (

    select *
    from "dev"."salesforce"."stg_salesforce__opportunity_tmp"
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
    
    
    amount
    
 as 
    
    amount
    
, 
    
    
    campaign_id
    
 as 
    
    campaign_id
    
, 
    
    
    close_date
    
 as 
    
    close_date
    
, 
    
    
    created_date
    
 as 
    
    created_date
    
, 
    
    
    description
    
 as 
    
    description
    
, 
    
    
    expected_revenue
    
 as 
    
    expected_revenue
    
, 
    
    
    fiscal
    
 as 
    
    fiscal
    
, 
    
    
    fiscal_quarter
    
 as 
    
    fiscal_quarter
    
, 
    
    
    fiscal_year
    
 as 
    
    fiscal_year
    
, 
    
    
    forecast_category
    
 as 
    
    forecast_category
    
, 
    
    
    forecast_category_name
    
 as 
    
    forecast_category_name
    
, 
    
    
    has_open_activity
    
 as 
    
    has_open_activity
    
, 
    
    
    has_opportunity_line_item
    
 as 
    
    has_opportunity_line_item
    
, 
    
    
    has_overdue_task
    
 as 
    
    has_overdue_task
    
, 
    
    
    id
    
 as 
    
    id
    
, 
    
    
    is_closed
    
 as 
    
    is_closed
    
, 
    
    
    is_deleted
    
 as 
    
    is_deleted
    
, 
    
    
    is_won
    
 as 
    
    is_won
    
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
    
    
    lead_source
    
 as 
    
    lead_source
    
, 
    
    
    name
    
 as 
    
    name
    
, 
    
    
    next_step
    
 as 
    
    next_step
    
, 
    
    
    owner_id
    
 as 
    
    owner_id
    
, 
    
    
    probability
    
 as 
    
    probability
    
, 
    
    
    record_type_id
    
 as 
    
    record_type_id
    
, 
    
    
    stage_name
    
 as 
    
    stage_name
    
, 
    
    
    synced_quote_id
    
 as 
    
    synced_quote_id
    
, 
    
    
    type
    
 as 
    
    type
    
, 
    
    
    demo_sat_date_c
    
 as demo_sat_date , 
    
    
    admin_opportunity_c
    
 as 
    
    admin_opportunity_c
    
, 
    cast(null as TEXT) as 
    
    became_mql_date
    
 , 
    
    
    existing_customer_revenue_type_c
    
 as 
    
    existing_customer_revenue_type_c
    
, 
    
    
    industry_c
    
 as 
    
    industry_c
    
, 
    
    
    lead_source_sub_type_c
    
 as 
    
    lead_source_sub_type_c
    
, 
    
    
    lead_source_type_c
    
 as 
    
    lead_source_type_c
    
, 
    
    
    lost_reason_c
    
 as 
    
    lost_reason_c
    
, 
    
    
    lost_sub_reason_c
    
 as 
    
    lost_sub_reason_c
    
, 
    
    
    opportunity_employees_c
    
 as 
    
    opportunity_employees_c
    
, 
    
    
    opportunity_originator_c
    
 as 
    
    opportunity_originator_c
    
, 
    
    
    originating_lead_id_c
    
 as 
    
    originating_lead_id_c
    
, 
    
    
    quote_arr_c
    
 as 
    
    quote_arr_c
    
, 
    
    
    quote_srr_c
    
 as 
    
    quote_srr_c
    
, 
    
    
    annual_recurring_revenue_c
    
 as 
    
    annual_recurring_revenue_c
    




    from base
), 

final as (
    
    select 
        cast(_fivetran_synced as timestamp) as _fivetran_synced,
        account_id,
        cast(amount as numeric(28,6)) as amount,
        campaign_id,
        cast(close_date as timestamp) as close_date,
        cast(created_date as timestamp) as created_date,
        description as opportunity_description,
        cast(expected_revenue as numeric(28,6)) as expected_revenue,
        fiscal,
        fiscal_quarter,
        fiscal_year,
        forecast_category,
        forecast_category_name,
        has_open_activity,
        has_opportunity_line_item,
        has_overdue_task,
        id as opportunity_id,
        is_closed,
        is_deleted,
        is_won,
        cast(last_activity_date as timestamp) as last_activity_date,
        cast(last_referenced_date as timestamp) as last_referenced_date,
        cast(last_viewed_date as timestamp) as last_viewed_date,
        lead_source,
        name as opportunity_name,
        next_step,
        owner_id,
        probability,
        record_type_id,
        stage_name,
        synced_quote_id,
        type
        
        


    
        
            
                , demo_sat_date
            
        
    
        
            
                , admin_opportunity_c
            
        
    
        
            
                , became_mql_date
            
        
    
        
            
                , existing_customer_revenue_type_c
            
        
    
        
            
                , industry_c
            
        
    
        
            
                , lead_source_sub_type_c
            
        
    
        
            
                , lead_source_type_c
            
        
    
        
            
                , lost_reason_c
            
        
    
        
            
                , lost_sub_reason_c
            
        
    
        
            
                , opportunity_employees_c
            
        
    
        
            
                , opportunity_originator_c
            
        
    
        
            
                , originating_lead_id_c
            
        
    
        
            
                , quote_arr_c
            
        
    
        
            
                , quote_srr_c
            
        
    
        
            
                , annual_recurring_revenue_c
            
        
    




    from fields
), 

calculated as (
        
    select 
        *,
        created_date >= date_trunc('month', getdate()) as is_created_this_month,
        created_date >= date_trunc('quarter', getdate()) as is_created_this_quarter,
        datediff(
        day,
        getdate(),
        created_date
        ) as days_since_created,
        datediff(
        day,
        close_date,
        created_date
        ) as days_to_close,
        date_trunc('month', close_date) = date_trunc('month', getdate()) as is_closed_this_month,
        date_trunc('quarter', close_date) = date_trunc('quarter', getdate()) as is_closed_this_quarter
    from final
)

select * 
from calculated
where not coalesce(is_deleted, false)