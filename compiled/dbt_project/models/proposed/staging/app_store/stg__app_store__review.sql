
with source as (

    select * from "dev"."app_store"."review"

),

renamed as (

    select
        app_id,
        id,
        date(last_modified) as date_day,
        rating,
        edited,
        title               as review_title,
        content             as review_content,
        total_views,
        helpful_views,
        nickname,
        app_version_string  as app_version,
        last_modified

    from source

)

select * from renamed