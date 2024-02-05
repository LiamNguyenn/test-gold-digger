

with source as (
    select *

    from "dev"."ebenefits"."subscription_active"

),

transformed as (
    select
        id::varchar                                                                                                                        as id, --noqa: RF04
        detail_type::varchar                                                                                                               as detail_type,
        source::varchar                                                                                                                    as source, --noqa: RF04
        time::timestamp                                                                                                                    as created_at,
        



    case when json_extract_path_text(detail, 'id')= '' then null else json_extract_path_text(detail, 'id') end as subscription_id
    
    ,

    case when json_extract_path_text(detail, 'userId')= '' then null else json_extract_path_text(detail, 'userId') end as ebenefits_user_uuid
    
    ,

    case when json_extract_path_text(detail, 'providerId')= '' then null else json_extract_path_text(detail, 'providerId') end as provider_id
    
    ,

    case when json_extract_path_text(detail, 'externalId')= '' then null else json_extract_path_text(detail, 'externalId') end as external_id
    
    ,

    case when json_extract_path_text(detail, 'externalUserId')= '' then null else json_extract_path_text(detail, 'externalUserId') end as external_user_id
    
    ,

    case when json_extract_path_text(detail, 'subscriptionType')= '' then null else json_extract_path_text(detail, 'subscriptionType') end as subscription_type
    
    



    from source
)

select * from transformed