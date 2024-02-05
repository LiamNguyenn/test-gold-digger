with source as (
    select *

    from "dev"."eh_engineering"."squad_members"
),

transformed as (
    select
        _row::bigint                                as row, -- noqa: RF04
        member_id::int                              as member_id,
        squad::varchar                              as squad,
        workstream::varchar                         as workstream,
        to_date(termination_date, 'YYYYMMDD')::date as termination_date,
        first_name::varchar                         as first_name,
        middle_name::varchar                        as middle_name,
        last_name::varchar                          as last_name,
        to_date(squad_added_date, 'YYYYMMDD')::date as squad_added_date,
        to_date(start_date, 'YYYYMMDD')::date       as start_date,
        archived::boolean                           as archived,
        _fivetran_synced::timestamp                 as fivetran_synced
    from source
)

select * from transformed