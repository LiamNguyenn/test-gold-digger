--To disable this model, set the using_user_role variable within your dbt_project.yml file to False.


with base as (

    select * 
    from "dev"."salesforce"."stg_salesforce__lead_tmp"
),

fields as (

    select
        
    
    
    _fivetran_synced
    
 as 
    
    _fivetran_synced
    
, 
    
    
    annual_revenue
    
 as 
    
    annual_revenue
    
, 
    
    
    city
    
 as 
    
    city
    
, 
    
    
    company
    
 as 
    
    company
    
, 
    
    
    converted_account_id
    
 as 
    
    converted_account_id
    
, 
    
    
    converted_contact_id
    
 as 
    
    converted_contact_id
    
, 
    
    
    converted_date
    
 as 
    
    converted_date
    
, 
    
    
    converted_opportunity_id
    
 as 
    
    converted_opportunity_id
    
, 
    
    
    country
    
 as 
    
    country
    
, 
    cast(null as TEXT) as 
    
    country_code
    
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
    
    
    email
    
 as 
    
    email
    
, 
    
    
    email_bounced_date
    
 as 
    
    email_bounced_date
    
, 
    
    
    email_bounced_reason
    
 as 
    
    email_bounced_reason
    
, 
    
    
    first_name
    
 as 
    
    first_name
    
, 
    
    
    has_opted_out_of_email
    
 as 
    
    has_opted_out_of_email
    
, 
    
    
    id
    
 as 
    
    id
    
, 
    
    
    individual_id
    
 as 
    
    individual_id
    
, 
    
    
    industry
    
 as 
    
    industry
    
, 
    
    
    is_converted
    
 as 
    
    is_converted
    
, 
    
    
    is_deleted
    
 as 
    
    is_deleted
    
, 
    
    
    is_unread_by_owner
    
 as 
    
    is_unread_by_owner
    
, 
    
    
    last_activity_date
    
 as 
    
    last_activity_date
    
, 
    
    
    last_modified_by_id
    
 as 
    
    last_modified_by_id
    
, 
    
    
    last_modified_date
    
 as 
    
    last_modified_date
    
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
    
    
    lead_source
    
 as 
    
    lead_source
    
, 
    
    
    master_record_id
    
 as 
    
    master_record_id
    
, 
    
    
    mobile_phone
    
 as 
    
    mobile_phone
    
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
    
    
    phone
    
 as 
    
    phone
    
, 
    
    
    postal_code
    
 as 
    
    postal_code
    
, 
    
    
    state
    
 as 
    
    state
    
, 
    cast(null as TEXT) as 
    
    state_code
    
 , 
    
    
    status
    
 as 
    
    status
    
, 
    
    
    street
    
 as 
    
    street
    
, 
    
    
    title
    
 as 
    
    title
    
, 
    
    
    website
    
 as 
    
    website
    
, 
    
    
    industry_primary_c
    
 as 
    
    industry_primary_c
    
, 
    
    
    industry_secondary_c
    
 as 
    
    industry_secondary_c
    
, 
    
    
    lost_reason_c
    
 as 
    
    lost_reason_c
    
, 
    
    
    lost_sub_reason_c
    
 as 
    
    lost_sub_reason_c
    
, 
    
    
    mql_score_c
    
 as 
    
    mql_score_c
    
, 
    cast(null as TEXT) as 
    
    numer_of_employees
    
 , 
    cast(null as TEXT) as 
    
    requested_demo_date_time
    
 , 
    cast(null as TEXT) as 
    
    sal_date
    
 , 
    
    
    sal_score_c
    
 as 
    
    sal_score_c
    
, 
    
    
    sql_score_c
    
 as 
    
    sql_score_c
    



        
    from base
), 

final as (
    
    select 
        cast(_fivetran_synced as timestamp) as _fivetran_synced,
        id as lead_id,
        annual_revenue,
        city,
        company,
        converted_account_id,
        converted_contact_id,
        cast(converted_date as timestamp) as converted_date,
        converted_opportunity_id,
        country,
        country_code,
        created_by_id,
        cast(created_date as timestamp) as created_date,
        description as lead_description,
        email,
        cast(email_bounced_date as timestamp) as email_bounced_date,
        email_bounced_reason,
        first_name,
        has_opted_out_of_email,
        individual_id,
        industry,
        is_converted,
        is_deleted,
        is_unread_by_owner,
        cast(last_activity_date as timestamp) as last_activity_date,
        last_modified_by_id,
        cast(last_modified_date as timestamp) as last_modified_date,
        last_name,
        cast(last_referenced_date as timestamp) as last_referenced_date,
        cast(last_viewed_date as timestamp) as last_viewed_date,
        lead_source,
        master_record_id,
        mobile_phone,
        name as lead_name,
        number_of_employees,
        owner_id,
        phone,
        postal_code,
        state,
        state_code,
        status,
        street,
        title,
        website
        
        


    
        
            
                , industry_primary_c
            
        
    
        
            
                , industry_secondary_c
            
        
    
        
            
                , lost_reason_c
            
        
    
        
            
                , lost_sub_reason_c
            
        
    
        
            
                , mql_score_c
            
        
    
        
            
                , numer_of_employees
            
        
    
        
            
                , requested_demo_date_time
            
        
    
        
            
                , sal_date
            
        
    
        
            
                , sal_score_c
            
        
    
        
            
                , sql_score_c
            
        
    



        
    from fields
)

select *
from final
where not coalesce(is_deleted, false)