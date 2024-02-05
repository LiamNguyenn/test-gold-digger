with source as (
    select *
    from "dev"."stg_checkly"."all_checks_results"
),

transformed as (
    select
        id::varchar                     as id, -- noqa: RF04
        has_errors::boolean             as has_errors,
        has_failures::boolean           as has_failures,
        run_location::varchar           as run_location,
        started_at::timestamp           as started_at,
        stopped_at::timestamp           as stopped_at,
        response_time::bigint           as response_time,
        check_id::varchar               as check_id,
        created_at::timestamp           as created_at,
        name::varchar                   as name, -- noqa: RF04
        check_run_id::bigint            as check_run_id,
        attempts::int                   as attempts,
        is_degraded::boolean            as is_degraded,
        over_max_response_time::boolean as over_max_response_time,
        result_type::varchar            as result_type,
        _transaction_date::timestamp    as transaction_date,
        _etl_date::timestamp            as etl_date
    from source
)

select * from transformed