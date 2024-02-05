




/* Build a dict of column names to column types */


     

     

     

     

     

     

     

     

     

     

     

     

     

     

     

     

     



    


with renamed as (
    select
        
            

            
    


            
                cast(user_uuid as TEXT)                                                          as external_id
            ,
        
            

            
    


            
                cast(event_id as TEXT)                                                          as event_id
            ,
        
            

            
    


            
                cast(event_time as timestamp)                                                          as updated_at
            ,
        
            

            
    


            
                cast(event_time as timestamp)                                                          as time
            ,
        
            

            
    


            
                cast(event_name as TEXT)                                                          as name
            ,
        
            

            
    


            
                cast(event_prop_device_type as TEXT)                                                          as device_type
            ,
        
            

            
    


            
                cast(event_prop_cashback_received as TEXT)                                                          as cashback_received
            ,
        
            

            
    


            
                
  
    case
        when event_prop_public_profile_enabled is true then 'true'
        when event_prop_public_profile_enabled is false then 'false'
    end::text

 as public_profile_enabled
            ,
        
            

            
    


            
                cast(event_prop_retailer as TEXT)                                                          as retailer
            ,
        
            

            
    


            
                cast(event_prop_shopnow_offer_category as TEXT)                                                          as shopnow_offer_category
            ,
        
            

            
    


            
                cast(event_prop_shopnow_offer_module as TEXT)                                                          as shopnow_offer_module
            ,
        
            

            
    


            
                cast(event_prop_shopnow_offer_type as TEXT)                                                          as shopnow_offer_type
            ,
        
            

            
    


            
                cast(event_prop_type_of_offer as TEXT)                                                          as type_of_offer
            ,
        
            

            
    


            
                to_char(event_prop_date_created at time zone 'UTC', 'yyyy-MM-ddTHH:mm:ss:SSSZ') as date_created
            
        
    from "dev"."exports"."exports_braze_user_events"
    
        where updated_at at time zone 'UTC' > (select max(updated_at) from "dev"."exports"."exports_braze_user_event_payloads")
    
),

/* Build a list of columns that are event properties */


    

    

    

    

    

    
        
    

    
        
    

    
        
    

    
        
    

    
        
    

    
        
    

    
        
    

    
        
    

    
        
    


    


new_kvpairs as (
    select
        event_id,
        updated_at,
        field_name,
        field_value
    from renamed unpivot (
        field_value for field_name in (
            
                device_type
                ,
            
                cashback_received
                ,
            
                public_profile_enabled
                ,
            
                retailer
                ,
            
                shopnow_offer_category
                ,
            
                shopnow_offer_module
                ,
            
                shopnow_offer_type
                ,
            
                type_of_offer
                ,
            
                date_created
                
            
        )
    )
),

payload as (
    select
        renamed.event_id,
        renamed.external_id,
        renamed.updated_at at time zone 'UTC'                                                                                                                                  as updated_at,
        '{' || '"name":' || '"' || renamed.name || '"' || ', ' || '"time": ' || '"' || renamed.time || '"' || ', ' || '"properties": ' || coalesce(p.payload_str, '{}') || '}' as payload
    from renamed
    left join (
        select
            event_id,
            '{' || listagg('"' || field_name || '":"' || field_value || '"', ', ') || '}' as payload_str
        from new_kvpairs
        group by 1
    ) as p
        on renamed.event_id = p.event_id
)

select
    event_id,
    external_id,
    updated_at,
    payload
from payload
where can_json_parse(payload)