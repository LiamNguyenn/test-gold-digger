












with renamed as (
    select
        event.message_id                               as event_id,
        event.user_id                                  as user_uuid,
        event.timestamp                                as event_time,
        decode(
            event.name,
            
                'Swag Profile - Populate from CV - Upload CV', 'candidate_cv_uploaded'
                ,
            
                'SWAG CV - Complete cv profile', 'candidate_profile_completed'
                ,
            
                'SWAG CV - Switched public on', 'candidate_public_profile'
                ,
            
                'Click shop now in specific online offer page', 'user_cashback_clicked_shop_now'
                
            
        )                                              as event_name,

        nullif(event.shopnow_offer_module, ''::text)   as shopnow_offer_module,
        nullif(event.shopnow_offer_type, ''::text)     as shopnow_offer_type,
        nullif(event.shopnow_offer_category, ''::text) as shopnow_offer_category
    from "dev"."customers"."events" as event
    


    -- this filter will only be applied in dev run
    
        where 1=1
    

        and event.name in ('Swag Profile - Populate from CV - Upload CV', 'SWAG CV - Complete cv profile', 'SWAG CV - Switched public on', 'Click shop now in specific online offer page')
),



all_events as (
    select * from renamed
    
),

enriched as (
    select
        e.*,
        c.public_profile as public_profile_enabled
    from all_events as e
    left join "dev"."ats"."candidate_profiles" as c
        on
            e.user_uuid = c.user_uuid
            and e.event_name in ('candidate_cv_uploaded', 'candidate_profile_completed', 'candidate_public_profile')
)

select * from enriched