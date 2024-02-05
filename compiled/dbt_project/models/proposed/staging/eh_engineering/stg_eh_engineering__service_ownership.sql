with source as (
    select *

    from "dev"."eh_engineering"."service_ownership"
),

transformed as (
    select
        _row::bigint                as row, -- noqa: RF04
        nonapplicable::boolean      as nonapplicable,
        squad::varchar              as squad,
        service::varchar            as service,
        platform::varchar           as platform,
        _fivetran_synced::timestamp as fivetran_synced

    from source
)

select * from transformed