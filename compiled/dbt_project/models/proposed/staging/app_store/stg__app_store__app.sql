

with source as (

    select * from "dev"."app_store"."app"

),

renamed as (

    select
        id   as app_id,
        is_enabled,
        name as app_name
    from source

)

select * from renamed