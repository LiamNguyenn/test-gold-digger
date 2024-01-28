with source as (

    select *
    from {{ source('eh_product', 'squad_board_ownership') }}

),

transformed as (

    select
        _row::bigint                          as row, -- noqa: RF04
        board_name::varchar                   as board_name,
        comments::varchar                     as comments, -- noqa: RF04
        include_in_product_reporting::varchar as include_in_product_reporting,
        kev_sian_reviewed::varchar            as kev_sian_reviewed,
        board_key::varchar                    as board_key,
        squad::varchar                        as squad,
        archived::varchar                     as archived,
        workstream::varchar                   as workstream,
        squad_owner::varchar                  as squad_owner,
        _fivetran_synced::timestamp           as fivetran_synced
    from source

)

select * from transformed
