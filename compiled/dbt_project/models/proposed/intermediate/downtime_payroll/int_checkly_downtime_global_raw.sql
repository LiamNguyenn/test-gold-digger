with downtime as (
    select
        id,
        name,
        check_type,
        current_check_started_at,
        current_check_stopped_at,
        next_check_started_at,
        datediff(
            'millisecond',
            current_check_started_at,
            least(
                next_check_started_at,
                dateadd(
                    'millisecond',
                    -1,
                    dateadd('day', 1, current_check_started_at::date) -- this is the end of the day for the check start time
                )
            )
        )::float / 1000                                      as downtime_check_start_date,
        lag(next_check_started_at) over (
            order by
                current_check_started_at
        )                                                    as checkpoint, -- noqa: RF04
        -- this is to compare the starting time of each check against the previous one's next checkpoint to determine if there's an overlap
        case
            when current_check_started_at < checkpoint
                then datediff(
                    'millisecond',
                    current_check_started_at,
                    least(
                        checkpoint,
                        dateadd(
                            'millisecond',
                            -1,
                            dateadd('day', 1, current_check_started_at::date) -- this is the end of the day for the check start time
                        ),
                        next_check_started_at
                    )
                )::float / 1000
            else 0
        end                                                  as overlap_check_start_date,
        downtime_check_start_date - overlap_check_start_date as net_downtime_check_start_date,
        case
            when current_check_started_at::date < current_check_stopped_at::date
                then datediff('millisecond', dateadd('day', 1, current_check_started_at::date), current_check_stopped_at)
            else 0
        end                                                  as downtime_check_end_date,
        case
            when current_check_started_at::date < current_check_stopped_at::date and current_check_stopped_at::date = checkpoint::date
                then datediff('millisecond', dateadd('day', 1, current_check_started_at::date), least(current_check_stopped_at, checkpoint))
            else 0
        end                                                  as overlap_check_end_date,
        downtime_check_end_date - overlap_check_end_date     as net_downtime_check_end_date
    from
        "dev"."intermediate"."int_checkly_results"
    where
        has_failures

),

qbo_au_downtime as (
    select
        id,
        name,
        check_type,
        current_check_started_at,
        current_check_stopped_at,
        next_check_started_at,
        datediff(
            'millisecond',
            current_check_started_at,
            least(
                next_check_started_at,
                dateadd(
                    'millisecond',
                    -1,
                    dateadd('day', 1, current_check_started_at::date) -- this is the end of the day for the check start time
                )
            )
        )::float / 1000                                      as downtime_check_start_date,
        lag(next_check_started_at) over (
            order by
                current_check_started_at
        )                                                    as checkpoint, -- noqa: RF04
        -- this is to compare the starting time of each check against the previous one's next checkpoint to determine if there's an overlap
        case
            when current_check_started_at < checkpoint
                then datediff(
                    'millisecond',
                    current_check_started_at,
                    least(
                        checkpoint,
                        dateadd(
                            'millisecond',
                            -1,
                            dateadd('day', 1, current_check_started_at::date) -- this is the end of the day for the check start time
                        ),
                        next_check_started_at
                    )
                )::float / 1000
            else 0
        end                                                  as overlap_check_start_date,
        downtime_check_start_date - overlap_check_start_date as net_downtime_check_start_date,
        case
            when current_check_started_at::date < current_check_stopped_at::date
                then datediff('millisecond', dateadd('day', 1, current_check_started_at::date), current_check_stopped_at)
            else 0
        end                                                  as downtime_check_end_date,
        case
            when current_check_started_at::date < current_check_stopped_at::date and current_check_stopped_at::date = checkpoint::date
                then datediff('millisecond', dateadd('day', 1, current_check_started_at::date), least(current_check_stopped_at, checkpoint))
            else 0
        end                                                  as overlap_check_end_date,
        downtime_check_end_date - overlap_check_end_date     as net_downtime_check_end_date
    from
        "dev"."intermediate"."int_checkly_results"
    where
        has_failures
        and name ~* 'AU'
        and case
            when lower(check_type) = 'browser' and name ~* 'QBO' and name ~* 'AU' and convert_timezone('Australia/Sydney', current_check_started_at)::time between '08:00:00' and '19:59:59' then TRUE -- Browser check in AU
            when lower(check_type) = 'api' and name ~* 'AU' and convert_timezone('Australia/Sydney', current_check_started_at)::time between '08:00:00' and '19:59:59' then TRUE -- API check in AU
        end
),

qbo_uk_downtime as (
    select
        id,
        name,
        check_type,
        current_check_started_at,
        current_check_stopped_at,
        next_check_started_at,
        datediff(
            'millisecond',
            current_check_started_at,
            least(
                next_check_started_at,
                dateadd(
                    'millisecond',
                    -1,
                    dateadd('day', 1, current_check_started_at::date) -- this is the end of the day for the check start time
                )
            )
        )::float / 1000                                      as downtime_check_start_date,
        lag(next_check_started_at) over (
            order by
                current_check_started_at
        )                                                    as checkpoint, -- noqa: RF04
        -- this is to compare the starting time of each check against the previous one's next checkpoint to determine if there's an overlap
        case
            when current_check_started_at < checkpoint
                then datediff(
                    'millisecond',
                    current_check_started_at,
                    least(
                        checkpoint,
                        dateadd(
                            'millisecond',
                            -1,
                            dateadd('day', 1, current_check_started_at::date) -- this is the end of the day for the check start time
                        ),
                        next_check_started_at
                    )
                )::float / 1000
            else 0
        end                                                  as overlap_check_start_date,
        downtime_check_start_date - overlap_check_start_date as net_downtime_check_start_date,
        case
            when current_check_started_at::date < current_check_stopped_at::date
                then datediff('millisecond', dateadd('day', 1, current_check_started_at::date), current_check_stopped_at)
            else 0
        end                                                  as downtime_check_end_date,
        case
            when current_check_started_at::date < current_check_stopped_at::date and current_check_stopped_at::date = checkpoint::date
                then datediff('millisecond', dateadd('day', 1, current_check_started_at::date), least(current_check_stopped_at, checkpoint))
            else 0
        end                                                  as overlap_check_end_date,
        downtime_check_end_date - overlap_check_end_date     as net_downtime_check_end_date
    from
        "dev"."intermediate"."int_checkly_results"
    where
        has_failures
        and name ~* 'UK'
        and case
            when lower(check_type) = 'browser' and name ~* 'QBO' and name ~* 'UK' and current_check_started_at::time between '08:00:00' and '17:59:59' then TRUE -- Browser check in the UK
            when lower(check_type) = 'api' and name ~* 'UK' and current_check_started_at::time between '08:00:00' and '17:59:59' then TRUE --API check in the UK
        end
)

select
    *,
    TRUE  as is_overall_downtime,
    FALSE as is_qbo_au_downtime,
    FALSE as is_qbo_uk_downtime

from downtime

union distinct

select
    *,
    FALSE as is_overall_downtime,
    TRUE  as is_qbo_au_downtime,
    FALSE as is_qbo_uk_downtime

from qbo_au_downtime

union distinct

select
    *,
    FALSE as is_overall_downtime,
    FALSE as is_qbo_au_downtime,
    TRUE  as is_qbo_uk_downtime

from qbo_uk_downtime