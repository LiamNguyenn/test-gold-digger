select
    results.id,
    checks.check_type,
    results.started_at as current_check_started_at,
    results.stopped_at as current_check_stopped_at,
    checks.name, -- for some dates and checks, the name in all_checks differs from all_checks_results for the same check_id in which case the name in all_checks is more informative and is thus preferred
    results.has_failures,
    results.has_errors,
    results.is_degraded,
    results.over_max_response_time,
    lead(current_check_started_at) over (
        partition by results.check_id
        order by
            current_check_started_at
    )                  as next_check_started_at
from
    {{ ref("stg_checkly__all_checks_results") }} as results
inner join {{ ref("stg_checkly__all_checks") }} as checks on results.check_id = checks.id
where
    checks.activated
    and results.started_at::date >= '2023-10-18'
    and checks.name !~* 'copy'
    and (checks.group_id not in (626035, 687601) or checks.group_id is NULL)
