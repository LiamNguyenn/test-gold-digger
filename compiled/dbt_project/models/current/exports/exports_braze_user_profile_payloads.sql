




    

    

    

    

    

    

    

    

    

    

    

    

    

    

    

    

    

    

    

    

    

    

    

    

    

    

    

    

    







with
renamed as (
    select
        
            
                
                
                    cast(user_uuid as TEXT)                                                   as external_id
                
            
            ,
        
            
                cast(dbt_updated_at as timestamp)                                             as updated_at
            
            ,
        
            
                
                
                    cast(email as TEXT)                                                   as email
                
            
            ,
        
            
                
                
                    cast(first_name as TEXT)                                                   as first_name
                
            
            ,
        
            
                
                
                    cast(last_name as TEXT)                                                   as last_name
                
            
            ,
        
            
                
                
                    cast(phone_number_e164 as TEXT)                                                   as phone
                
            
            ,
        
            
                
                
                    cast(alpha_two_letter as TEXT)                                                   as country
                
            
            ,
        
            
                
                
                    cast(gender as TEXT)                                                   as gender
                
            
            ,
        
            
                
                
                    cast(home_city as TEXT)                                                   as home_city
                
            
            ,
        
            
                
                
                    case when user_is_candidate then 'true' else 'false' end                  as user_is_candidate
                
            
            ,
        
            
                
                
                    to_char(user_date_created at time zone 'UTC', 'yyyy-MM-ddTHH:mm:ss:SSSZ') as user_date_created
                
            
            ,
        
            
                
                
                    case when user_actively_employed then 'true' else 'false' end                  as user_actively_employed
                
            
            ,
        
            
                
                
                    cast(postcode as TEXT)                                                   as postcode
                
            
            ,
        
            
                
                
                    cast(state_code as TEXT)                                                   as state
                
            
            ,
        
            
                
                
                    cast(candidate_recent_job_title as TEXT)                                                   as candidate_job_title
                
            
            ,
        
            
                cast('true' as TEXT)                                                          as swag_app_workspace_user
            
            
        
    from "dev"."snapshots"."exports_braze_users_snapshot"
    where
        1 = 1
        
            -- this filter will only be applied on an incremental run
            and dbt_updated_at at time zone 'UTC' > (select max(updated_at) from "dev"."exports"."exports_braze_user_profile_payloads")
        
        and dbt_valid_to is NULL
),

new_kvpairs as (
    select *
    from (
        select
            external_id,
            updated_at,
            
                email
                ,
            
                first_name
                ,
            
                last_name
                ,
            
                phone
                ,
            
                country
                ,
            
                gender
                ,
            
                home_city
                ,
            
                user_is_candidate
                ,
            
                user_date_created
                ,
            
                user_actively_employed
                ,
            
                postcode
                ,
            
                state
                ,
            
                candidate_job_title
                ,
            
                swag_app_workspace_user
                
            
        from renamed
    ) unpivot (
        value for key in (
            
                email
                ,
            
                first_name
                ,
            
                last_name
                ,
            
                phone
                ,
            
                country
                ,
            
                gender
                ,
            
                home_city
                ,
            
                user_is_candidate
                ,
            
                user_date_created
                ,
            
                user_actively_employed
                ,
            
                postcode
                ,
            
                state
                ,
            
                candidate_job_title
                ,
            
                swag_app_workspace_user
                
            
        )
    )
)


-- Below pulls in existing records and flattens them
    , loaded_kvpairs as (
        select
            external_id,
            key,
            value,
            updated_at
        from (select
            external_id,
            updated_at,
            json_parse(payload) as payload
        from "dev"."exports"."exports_braze_user_profile_payloads") as t,
            unpivot t.payload as value at key
        where 1 = 1
        qualify row_number() over (partition by external_id, key order by updated_at desc) = 1
    )


-- Compares the most recent records currently transmitted to Braze, by key and external_id
-- If the new data is different from the existing data, that data will be transmitted
-- Only the individual cells that have changed will be sent, or external_ids that are completely new
, json_payloads as (
    select
        nkv.external_id,
        '{' || listagg('"' || nkv.key || '":"' || nkv.value || '"', ', ') || '}' as payload_str,
        max(nkv.updated_at)                                                      as updated_at
    from new_kvpairs as nkv
    
        left join loaded_kvpairs as lkv on nkv.external_id = lkv.external_id and nkv.key = lkv.key
        where coalesce(nkv.value, '|') != coalesce(lkv.value, '|')
    
    group by 1
)

select
    external_id,
    payload_str                   as payload,
    updated_at at time zone 'UTC' as updated_at
from json_payloads
where can_json_parse(payload_str)