with source as (
    select *

    from "dev"."heroshop_db_public"."suppliers"
),

transformed as (
    select
        id::varchar                as id, -- noqa: RF04
        name::varchar              as name, -- noqa: RF04
        _fivetran_deleted::boolean as fivetran_deleted

    from source
)

select * from transformed
where not fivetran_deleted