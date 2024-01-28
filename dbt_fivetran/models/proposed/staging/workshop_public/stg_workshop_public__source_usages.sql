with source_usages as (
    select *
    from {{ source("workshop_public", "source_usages") }}
),

transformed as (
    select
        id::varchar                     as id, -- noqa: RF04
        to_date(date, 'YYYYMMDD')::date as date, -- noqa: RF04
        source::varchar                 as source, -- noqa: RF04
        count::int                      as source_usage_count,
        squad::varchar                  as squad,
        created_at::timestamp           as created_at,
        updated_at::timestamp           as updated_at

    from source_usages
    where not _fivetran_deleted

)

select * from transformed
