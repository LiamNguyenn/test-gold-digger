with source as (
    select *

    from "dev"."workshop_public"."pingdom_check_stats"
),

transformed as (
    select
        id::varchar               as id, -- noqa: RF04
        up_time::int              as up_time,
        down_time::int            as down_time,
        unknown_time::int         as unknown_time,
        avg_response::int         as avg_response_time,
        date::date                as date, -- noqa: RF04
        pingdom_check_id::varchar as pingdom_check_id,
        created_at::timestamp     as created_at,
        updated_at::timestamp     as updated_at

    from source
    where not _fivetran_deleted
)

select * from transformed