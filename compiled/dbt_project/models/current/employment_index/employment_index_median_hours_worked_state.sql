

with
    combined_platform_aus as (
        select * from "dev"."employment_index"."eh_kp_combined_hours"
    ),
    med_hours_worked as (
        select distinct
            residential_state,
            month,
            median(monthly_hours) over (
                partition by residential_state, month
            ) as median_hours_worked
        from combined_platform_aus
        where residential_state is not null
        order by residential_state, month
    ),
    monthly_change as (
        select
            residential_state,
            month,
            median_hours_worked,
            lag(median_hours_worked) over (
                partition by residential_state order by residential_state, month
            ) as previous_month_lag,
            case
                when median_hours_worked = 0
                then 0
                else
                    round(
                        (median_hours_worked - previous_month_lag)
                        / previous_month_lag,
                        3
                    )
            end as hours_worked_growth_monthly
        from med_hours_worked
        order by residential_state, month
    ),
    quarterly_change as (
        select
            residential_state,
            month,
            median_hours_worked,
            lag(median_hours_worked, 3) over (
                partition by residential_state order by residential_state, month
            ) as previous_quarter_lag,
            case
                when median_hours_worked = 0
                then 0
                else
                    round(
                        (median_hours_worked - previous_quarter_lag)
                        / previous_quarter_lag,
                        3
                    )
            end as hours_worked_growth_quarterly
        from med_hours_worked
        order by residential_state, month
    ),
    semiannual_change as (
        select
            residential_state,
            month,
            median_hours_worked,
            lag(median_hours_worked, 6) over (
                partition by residential_state order by residential_state, month
            ) as previous_semiannual_lag,
            case
                when median_hours_worked = 0
                then 0
                else
                    round(
                        (median_hours_worked - previous_semiannual_lag)
                        / previous_semiannual_lag,
                        3
                    )
            end as hours_worked_growth_semiannual
        from med_hours_worked
        order by residential_state, month
    ),
    yearly_change as (
        select
            residential_state,
            month,
            median_hours_worked,
            lag(median_hours_worked, 12) over (
                partition by residential_state order by residential_state, month
            ) as previous_yearly_lag,
            round(
                (median_hours_worked - previous_yearly_lag) / previous_yearly_lag, 3
            ) as hours_worked_growth_yearly
        from med_hours_worked
        order by residential_state, month
    ),
    min_sample_size as (
        select
            residential_state,
            month,
            count(distinct organisation_id) as business_sample,
            count(distinct member_id) as employee_sample
        from combined_platform_aus
        group by 1, 2
        having business_sample > 150
    )

select
    m.residential_state,
    m.month,
    m.median_hours_worked,
    previous_month_lag,
    hours_worked_growth_monthly,
    previous_quarter_lag,
    hours_worked_growth_quarterly,
    previous_semiannual_lag,
    hours_worked_growth_semiannual,
    previous_yearly_lag,
    hours_worked_growth_yearly
from monthly_change m
left join
    quarterly_change q
    on (m.month = q.month and m.residential_state = q.residential_state)
left join
    semiannual_change s
    on (m.month = s.month and m.residential_state = s.residential_state)
left join
    yearly_change y on (m.month = y.month and m.residential_state = y.residential_state)
join
    min_sample_size ms
    on (m.month = ms.month and m.residential_state = ms.residential_state)
order by m.residential_state, m.month