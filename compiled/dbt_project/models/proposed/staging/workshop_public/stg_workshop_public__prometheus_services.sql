with source as (
    select *

    from "dev"."workshop_public"."prometheus_services"
),

transformed as (
    select
        id::varchar           as id, -- noqa: RF04
        name::varchar         as name, -- noqa: RF04
        created_at::timestamp as created_at,
        updated_at::timestamp as updated_at

    from source
    where not _fivetran_deleted

)

select * from transformed