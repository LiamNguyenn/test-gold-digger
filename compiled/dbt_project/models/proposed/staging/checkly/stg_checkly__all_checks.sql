with source as (
    select *

    from "dev"."stg_checkly"."all_checks"
),

transformed as (
    select
        id::varchar                        as id, -- noqa: RF04
        check_type::varchar                as check_type,
        name::varchar                      as name, -- noqa: RF04
        frequency::int                     as frequency,
        frequency_offset::int              as frequency_offset,
        activated::boolean                 as activated,
        muted::boolean                     as muted,
        should_fail::boolean               as should_fail,
        locations::varchar                 as locations,
        script::varchar                    as script,
        created_at::timestamp              as created_at,
        updated_at::timestamp              as updated_at,
        double_check::boolean              as double_check,
        ssl_check_domain::varchar          as ssl_check_domain,
        tear_down_snippet_id::int          as tear_down_snippet_id,
        local_setup_script::varchar        as local_setup_script,
        local_tear_down_script::varchar    as local_tear_down_script,
        use_global_alert_settings::boolean as use_global_alert_settings,
        degraded_response_time::int        as degraded_response_time,
        max_response_time::int             as max_response_time,
        group_id::int                      as group_id,
        group_order::int                   as group_order,
        runtime_id::boolean                as runtime_id,
        script_path::boolean               as script_path,
        _transaction_date::timestamp       as transaction_date,
        _etl_date::timestamp               as etl_date,
        setup_snippet_id::int              as setup_snippet_id,
        run_parallel::boolean              as run_parallel

    from source
)

select * from transformed