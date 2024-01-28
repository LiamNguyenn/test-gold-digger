{% set columns_list = [
    {"ehUUId": "eh_user_uuid"},
    {"kpId": "keypay_user_id"},
    {"eBenUUId": "ebenefits_user_uuid"},
    {"emailAddress": "email"}
] %}

with source as (
    select *

    from {{ source("ebenefits", "user_created") }}
),

transformed as (
    select
        id::varchar                                                                                                                as id, --noqa: RF04
        detail_type::varchar                                                                                                       as detail_type,
        source::varchar                                                                                                            as source, --noqa: RF04
        time::timestamp                                                                                                            as created_at,
        {{ extract_all_json_elements(columns_list, "detail") }}

    from source
)

select * from transformed
