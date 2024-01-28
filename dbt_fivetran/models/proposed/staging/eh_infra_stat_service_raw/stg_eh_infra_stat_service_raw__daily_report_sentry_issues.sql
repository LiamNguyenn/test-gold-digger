with source as (
    select *

    from {{ source("eh_infra_stat_service_raw", "daily_report_sentry_issues") }}
),

transformed as (
    select
        id::varchar                           as id, -- noqa: RF04
        squad_owner::varchar                  as squad_owner,
        environment::varchar                  as environment,
        to_date(date_index, 'YYYYMMDD')::date as date_index,
        platform::varchar                     as platform,
        first_seen::datetime                  as first_seen,
        last_seen::datetime                   as last_seen,
        first_seen_to_last_seen::int          as first_seen_to_last_seen,
        user_count::int                       as user_count,
        num_comments::int                     as num_comments,
        title::varchar                        as title,
        culprit::varchar                      as culprit,
        assigned_to_email::varchar            as assigned_to_email,
        assigned_to_id::varchar               as assigned_to_id,
        assigned_to_type::varchar             as assigned_to_type,
        status::varchar                       as status, -- noqa: RF04
        source_type::varchar                  as source_type,
        has_seen::boolean                     as has_seen,
        is_public::boolean                    as is_public,
        short_id::varchar                     as short_id,
        event_count::int                      as event_count,
        permalink::varchar                    as permalink,
        level::varchar                        as level, -- noqa: RF04
        is_subscribed::boolean                as is_subscribed,
        is_bookmarked::boolean                as is_bookmarked,
        project_id::int                       as project_id,
        project_name::varchar                 as project_name,
        project_slug::varchar                 as project_slug,
        source_created_at::timestamp          as source_created_at,
        source_updated_at::timestamp          as source_updated_at,
        issues_id::bigint                     as issues_id,
        created_at::timestamp                 as created_at,
        updated_at::timestamp                 as updated_at,
        _fivetran_synced::timestamp           as fivetran_synced
    from source
    where not _fivetran_deleted

)

select * from transformed
