with source as (
    select *

    from "dev"."workshop_public"."prometheus_service_stats"
),

transformed as (
    select
        id::varchar                   as id, -- noqa: RF04
        p50::int                      as p50,
        p90::int                      as p90,
        date::date                    as date, -- noqa: RF04
        prometheus_service_id::bigint as prometheus_service_id,
        created_at::timestamp         as created_at,
        updated_at::timestamp         as updated_at

    from source
    where not _fivetran_deleted

)

select * from transformed