{{ config(materialized='view', schema='exports') }}

with renamed as (
    select
        u.uuid                            as user_uuid,
        'candidate_account_created'::text as event_name,
        uinfo.updated_at                  as event_time,
        uinfo.created_at                  as date_created
    from {{ source('postgres_public', 'user_infos') }} as uinfo
    left join {{ source('postgres_public', 'users') }} as u
        on uinfo.user_id = u.id
    where
        1 = 1
        and source = 'career_page'
        and not coalesce(uinfo._fivetran_deleted, true)
)

select
    {{ dbt_utils.generate_surrogate_key(['user_uuid', 'event_name', 'event_time']) }} as event_id,
    *
from renamed
