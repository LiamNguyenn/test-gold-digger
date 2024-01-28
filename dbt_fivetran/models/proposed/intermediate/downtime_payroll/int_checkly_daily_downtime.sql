with downtime as (
    select *

    from {{ ref("int_checkly_downtime_global_raw") }}
),

check_start_date_downtime as (
    select
        current_check_started_at::date                                                   as date, -- noqa: RF04
        name,
        sum(case when is_overall_downtime then net_downtime_check_start_date else 0 end) as total_net_downtime_same_day,
        sum(case when is_overall_downtime then downtime_check_start_date else 0 end)     as total_downtime_same_day,
        sum(case when is_qbo_au_downtime then net_downtime_check_start_date else 0 end)  as qbo_au_net_downtime_same_day,
        sum(case when is_qbo_au_downtime then downtime_check_start_date else 0 end)      as qbo_au_downtime_same_day,
        sum(case when is_qbo_uk_downtime then net_downtime_check_start_date else 0 end)  as qbo_uk_net_downtime_same_day,
        sum(case when is_qbo_uk_downtime then downtime_check_start_date else 0 end)      as qbo_uk_downtime_same_day
    from downtime
    group by 1, 2
),

check_end_date_downtime as (
    select
        current_check_stopped_at::date                                                 as date, -- noqa: RF04
        name,
        sum(case when is_overall_downtime then net_downtime_check_end_date else 0 end) as total_net_downtime_next_day,
        sum(case when is_overall_downtime then downtime_check_end_date else 0 end)     as total_downtime_next_day,
        sum(case when is_qbo_au_downtime then net_downtime_check_end_date else 0 end)  as qbo_au_net_downtime_next_day,
        sum(case when is_qbo_au_downtime then downtime_check_end_date else 0 end)      as qbo_au_downtime_next_day,
        sum(case when is_qbo_uk_downtime then net_downtime_check_end_date else 0 end)  as qbo_uk_net_downtime_next_day,
        sum(case when is_qbo_uk_downtime then downtime_check_end_date else 0 end)      as qbo_uk_downtime_next_day
    from downtime
    group by 1, 2
),

combined as (
    select
        intcr.current_check_started_at::date                                                                      as date, -- noqa: RF04
        intcr.name,
        coalesce(max(csdd.total_net_downtime_same_day), 0) + coalesce(max(cedd.total_net_downtime_next_day), 0)   as total_net_downtime,
        coalesce(max(csdd.total_downtime_same_day), 0) + coalesce(max(cedd.total_downtime_next_day), 0)           as total_downtime,
        coalesce(max(csdd.qbo_au_net_downtime_same_day), 0) + coalesce(max(cedd.qbo_au_net_downtime_next_day), 0) as qbo_au_net_downtime,
        coalesce(max(csdd.qbo_au_downtime_same_day), 0) + coalesce(max(cedd.qbo_au_downtime_next_day), 0)         as qbo_au_downtime,
        coalesce(max(csdd.qbo_uk_net_downtime_same_day), 0) + coalesce(max(cedd.qbo_uk_net_downtime_next_day), 0) as qbo_uk_net_downtime,
        coalesce(max(csdd.qbo_uk_downtime_same_day), 0) + coalesce(max(cedd.qbo_uk_downtime_next_day), 0)         as qbo_uk_downtime
    from {{ ref("int_checkly_results") }} as intcr

    left join check_start_date_downtime as csdd
        on
            csdd.date = intcr.current_check_started_at::date
            and (intcr.name = csdd.name or csdd.name is NULL)

    left join check_end_date_downtime as cedd
        on
            cedd.date = intcr.current_check_started_at::date
            and (intcr.name = cedd.name or cedd.name is NULL)

    group by 1, 2
)

select * from combined
