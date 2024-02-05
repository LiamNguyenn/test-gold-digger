with source as (
    select *

    from "dev"."postgres_public"."user_infos"
),

transformed as (
    select
        id::varchar                       as id, --noqa: RF04
        user_id::varchar                  as user_id,
        first_name::varchar               as first_name,
        last_name::varchar                as last_name,
        user_verified_at::timestamp       as verified_at,
        activated_at::timestamp           as activated_at,
        marketing_consented_at::timestamp as marketing_consented_at,
        completed_profile::boolean        as is_profile_completed,
        public_profile::boolean           as is_public_profile,
        created_at::timestamp             as created_at,
        _fivetran_synced::timestamp       as fivetran_synced

    from source

    where not _fivetran_deleted
)

select * from transformed