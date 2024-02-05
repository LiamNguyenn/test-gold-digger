with source as (
    select *

    from "dev"."eh_product"."workstream_ownership"
),

transformed as (
    select
        _row::int                   as row, -- noqa: RF04
        product_owner::varchar      as product_owner,
        product_escalation::varchar as product_escalation,
        product_family::varchar     as product_family,
        product_line::varchar       as product_line,
        workstream::varchar         as workstream,
        _fivetran_synced::timestamp as fivetran_synced
    from source
)

select * from transformed