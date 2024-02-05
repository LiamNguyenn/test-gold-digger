with base as (

    select * 
    from "dev"."salesforce"."stg_salesforce__contact_tmp"
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
    
    
    department
    
 as 
    
    department
    
, 
    
    
    description
    
 as 
    
    description
    
, 
    
    
    email
    
 as 
    
    email
    
, 
    
    
    first_name
    
 as 
    
    first_name
    
, 
    
    
    home_phone
    
 as 
    
    home_phone
    
, 
    
    
    id
    
 as 
    
    id
    
, 
    
    
    individual_id
    
 as 
    
    individual_id
    
, 
    
    
    is_deleted
    
 as 
    
    is_deleted
    
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
    
    
    mailing_city
    
 as 
    
    mailing_city
    
, 
    
    
    mailing_country
    
 as 
    
    mailing_country
    
, 
    cast(null as TEXT) as 
    
    mailing_country_code
    
 , 
    
    
    mailing_postal_code
    
 as 
    
    mailing_postal_code
    
, 
    
    
    mailing_state
    
 as 
    
    mailing_state
    
, 
    cast(null as TEXT) as 
    
    mailing_state_code
    
 , 
    
    
    mailing_street
    
 as 
    
    mailing_street
    
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
    
    
    owner_id
    
 as 
    
    owner_id
    
, 
    
    
    phone
    
 as 
    
    phone
    
, 
    
    
    reports_to_id
    
 as 
    
    reports_to_id
    
, 
    
    
    title
    
 as 
    
    title
    



        
    from base
), 

final as (
    
    select 
        cast(_fivetran_synced as timestamp) as _fivetran_synced,
        id as contact_id,
        account_id,
        department,
        description as contact_description,
        email,
        first_name,
        home_phone,
        individual_id,
        is_deleted,
        cast(last_activity_date as timestamp) as last_activity_date,
        last_modified_by_id,
        last_modified_date,
        last_name,
        last_referenced_date,
        cast(last_viewed_date as timestamp) as last_viewed_date,
        lead_source,
        mailing_city,
        mailing_country,
        mailing_country_code,
        mailing_postal_code,
        mailing_state,
        mailing_state_code,
        mailing_street,
        master_record_id,
        mobile_phone,
        name as contact_name,
        owner_id,
        phone,
        reports_to_id,
        title
        
        




        
    from fields
)

select *
from final
where not coalesce(is_deleted, false)