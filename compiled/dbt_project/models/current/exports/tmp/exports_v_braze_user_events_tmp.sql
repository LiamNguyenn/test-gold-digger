-- Generate union view
  with base as (
    

        (
            select
                cast('"dev"."exports"."int_exports__sign_in_app_events"' as TEXT) as _dbt_source_relation,

                
                    cast("event_id" as character varying(256)) as "event_id" ,
                    cast("user_uuid" as character varying(4096)) as "user_uuid" ,
                    cast("event_name" as character varying(256)) as "event_name" ,
                    cast("event_time" as timestamp with time zone) as "event_time" ,
                    cast("device" as character varying(256)) as "device" ,
                    cast(null as character varying(4096)) as "shopnow_offer_module" ,
                    cast(null as character varying(4096)) as "shopnow_offer_type" ,
                    cast(null as character varying(4096)) as "shopnow_offer_category" ,
                    cast(null as boolean) as "public_profile_enabled" ,
                    cast(null as character varying(256)) as "retailer" ,
                    cast(null as character varying(8)) as "type_of_offer" ,
                    cast(null as double precision) as "cashback_received" ,
                    cast(null as timestamp without time zone) as "date_created" ,
                    cast(null as character varying(2048)) as "certification_name" ,
                    cast(null as date) as "certification_issue_date" ,
                    cast(null as date) as "certification_end_date" 

            from "dev"."exports"."int_exports__sign_in_app_events"

            
        )

        union all
        

        (
            select
                cast('"dev"."exports"."int_exports__swag_user_events"' as TEXT) as _dbt_source_relation,

                
                    cast("event_id" as character varying(256)) as "event_id" ,
                    cast("user_uuid" as character varying(4096)) as "user_uuid" ,
                    cast("event_name" as character varying(256)) as "event_name" ,
                    cast("event_time" as timestamp with time zone) as "event_time" ,
                    cast(null as character varying(256)) as "device" ,
                    cast("shopnow_offer_module" as character varying(4096)) as "shopnow_offer_module" ,
                    cast("shopnow_offer_type" as character varying(4096)) as "shopnow_offer_type" ,
                    cast("shopnow_offer_category" as character varying(4096)) as "shopnow_offer_category" ,
                    cast("public_profile_enabled" as boolean) as "public_profile_enabled" ,
                    cast(null as character varying(256)) as "retailer" ,
                    cast(null as character varying(8)) as "type_of_offer" ,
                    cast(null as double precision) as "cashback_received" ,
                    cast(null as timestamp without time zone) as "date_created" ,
                    cast(null as character varying(2048)) as "certification_name" ,
                    cast(null as date) as "certification_issue_date" ,
                    cast(null as date) as "certification_end_date" 

            from "dev"."exports"."int_exports__swag_user_events"

            
        )

        union all
        

        (
            select
                cast('"dev"."exports"."int_exports__swag_cashback_transactions"' as TEXT) as _dbt_source_relation,

                
                    cast("event_id" as character varying(256)) as "event_id" ,
                    cast("user_uuid" as character varying(4096)) as "user_uuid" ,
                    cast("event_name" as character varying(256)) as "event_name" ,
                    cast("event_time" as timestamp with time zone) as "event_time" ,
                    cast(null as character varying(256)) as "device" ,
                    cast(null as character varying(4096)) as "shopnow_offer_module" ,
                    cast(null as character varying(4096)) as "shopnow_offer_type" ,
                    cast(null as character varying(4096)) as "shopnow_offer_category" ,
                    cast(null as boolean) as "public_profile_enabled" ,
                    cast("retailer" as character varying(256)) as "retailer" ,
                    cast("type_of_offer" as character varying(8)) as "type_of_offer" ,
                    cast("cashback_received" as double precision) as "cashback_received" ,
                    cast(null as timestamp without time zone) as "date_created" ,
                    cast(null as character varying(2048)) as "certification_name" ,
                    cast(null as date) as "certification_issue_date" ,
                    cast(null as date) as "certification_end_date" 

            from "dev"."exports"."int_exports__swag_cashback_transactions"

            
        )

        union all
        

        (
            select
                cast('"dev"."exports"."int_exports__jobs_swagapp_dotcom_events"' as TEXT) as _dbt_source_relation,

                
                    cast("event_id" as character varying(256)) as "event_id" ,
                    cast("user_uuid" as character varying(4096)) as "user_uuid" ,
                    cast("event_name" as character varying(256)) as "event_name" ,
                    cast("event_time" as timestamp with time zone) as "event_time" ,
                    cast(null as character varying(256)) as "device" ,
                    cast(null as character varying(4096)) as "shopnow_offer_module" ,
                    cast(null as character varying(4096)) as "shopnow_offer_type" ,
                    cast(null as character varying(4096)) as "shopnow_offer_category" ,
                    cast(null as boolean) as "public_profile_enabled" ,
                    cast(null as character varying(256)) as "retailer" ,
                    cast(null as character varying(8)) as "type_of_offer" ,
                    cast(null as double precision) as "cashback_received" ,
                    cast("date_created" as timestamp without time zone) as "date_created" ,
                    cast("certification_name" as character varying(2048)) as "certification_name" ,
                    cast("certification_issue_date" as date) as "certification_issue_date" ,
                    cast("certification_end_date" as date) as "certification_end_date" 

            from "dev"."exports"."int_exports__jobs_swagapp_dotcom_events"

            
        )

        
  ),

  renamed as (
    select
      event_id,
      event_name,
      event_time,
      user_uuid,
      _dbt_source_relation,
          device                 as event_prop_device_type,
          public_profile_enabled                 as event_prop_public_profile_enabled,
          shopnow_offer_module                 as event_prop_shopnow_offer_module,
          shopnow_offer_type                 as event_prop_shopnow_offer_type,
          shopnow_offer_category                 as event_prop_shopnow_offer_category,
          retailer                 as event_prop_retailer,
          type_of_offer                 as event_prop_type_of_offer,
          cashback_received                 as event_prop_cashback_received,
          date_created                 as event_prop_date_created,
          certification_name                 as event_prop_certification_name,
          certification_issue_date                 as event_prop_certification_issue_date,
          certification_end_date                 as event_prop_certification_end_date
    from base
  )

  select * from renamed