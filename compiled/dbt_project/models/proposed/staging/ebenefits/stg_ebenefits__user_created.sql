

with source as (
    select *

    from "dev"."ebenefits"."user_created"
),

transformed as (
    select
        id::varchar                                                                                                                as id, --noqa: RF04
        detail_type::varchar                                                                                                       as detail_type,
        source::varchar                                                                                                            as source, --noqa: RF04
        time::timestamp                                                                                                            as created_at,
        



    case when json_extract_path_text(detail, 'ehUUId')= '' then null else json_extract_path_text(detail, 'ehUUId') end as eh_user_uuid
    
    ,

    case when json_extract_path_text(detail, 'kpId')= '' then null else json_extract_path_text(detail, 'kpId') end as keypay_user_id
    
    ,

    case when json_extract_path_text(detail, 'eBenUUId')= '' then null else json_extract_path_text(detail, 'eBenUUId') end as ebenefits_user_uuid
    
    ,

    case when json_extract_path_text(detail, 'emailAddress')= '' then null else json_extract_path_text(detail, 'emailAddress') end as email
    
    



    from source
)

select * from transformed