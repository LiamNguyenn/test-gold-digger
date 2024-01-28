with source as (

    select *
    from {{ source('github', 'team') }}

),

transformed as (

    select
        id::bigint                  as id, -- noqa: RF04
        name::varchar               as name, -- noqa: RF04
        slug::varchar               as slug,
        description::varchar        as description,
        privacy::varchar            as privacy,
        org_id::bigint              as org_id,
        parent_id::bigint           as parent_id,
        _fivetran_synced::timestamp as fivetran_synced

    from source

)

select * from transformed
