with source as (
    select *

    from "dev"."postgres_public"."users"
),

transformed as (
    select
        id::int                     as id,  --noqa: RF04
        uuid::varchar               as uuid,
        email::varchar              as email,
        acknowledged_tnc::boolean   as has_acknowledged_eh_tnc,
        twofa_enabled::boolean      as is_twofa_enabled,
        created_at::timestamp       as created_at,
        _fivetran_synced::timestamp as fivetran_synced


    from source

    where
        not _fivetran_deleted
        and not is_shadow_data
)

select * from transformed