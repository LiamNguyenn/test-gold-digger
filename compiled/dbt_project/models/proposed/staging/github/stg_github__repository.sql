with source as (

    select *
    from "dev"."github"."repository"

),

transformed as (
    select
        id::bigint                  as id, -- noqa: RF04
        name::varchar               as name, -- noqa: RF04
        full_name::varchar          as full_name,
        description::varchar        as description,
        fork::boolean               as fork,
        archived::boolean           as archived,
        homepage::varchar           as homepage,
        language::varchar           as language, -- noqa: RF04
        default_branch::varchar     as default_branch,
        created_at::timestamp       as created_at,
        owner_id::bigint            as owner_id,
        private::boolean            as private, -- noqa: RF04
        _fivetran_synced::timestamp as fivetran_synced,
        stargazers_count::int       as stargazers_count

    from source
)

select * from transformed