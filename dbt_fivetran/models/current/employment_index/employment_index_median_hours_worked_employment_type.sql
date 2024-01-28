{{ config(alias="median_hours_worked_employment_type") }}

with
    combined_platform_aus as (
        select * from {{ ref("employment_index_eh_kp_combined_hours") }}
    ),
    med_hours_worked as (
        select distinct
            employment_type,
            month,
            median(monthly_hours) over (
                partition by employment_type, month
            ) as median_hours_worked
        from combined_platform_aus
        where employment_type is not null
        order by employment_type, month
    ),
    monthly_change as (
        select
            employment_type,
            month,
            median_hours_worked,
            lag(median_hours_worked) over (
                partition by employment_type order by employment_type, month
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
        order by employment_type, month
    ),

    quarterly_change as (
        select
            employment_type,
            month,
            median_hours_worked,
            lag(median_hours_worked, 3) over (
                partition by employment_type order by employment_type, month
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
        order by employment_type, month
    ),
    semiannual_change as (
        select
            employment_type,
            month,
            median_hours_worked,
            lag(median_hours_worked, 6) over (
                partition by employment_type order by employment_type, month
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
        order by employment_type, month
    ),
    yearly_change as (
        select
            employment_type,
            month,
            median_hours_worked,
            lag(median_hours_worked, 12) over (
                partition by employment_type order by employment_type, month
            ) as previous_yearly_lag,
            case
                when median_hours_worked = 0
                then 0
                else
                    round(
                        (median_hours_worked - previous_yearly_lag)
                        / previous_yearly_lag,
                        3
                    )
            end as hours_worked_growth_yearly
        from med_hours_worked
        order by employment_type, month
    ),
    min_sample_size as (
        select
            employment_type,
            month,
            count(distinct organisation_id) as business_sample,
            count(distinct member_id) as employee_sample
        from combined_platform_aus
        group by 1, 2
        having business_sample > 150
    )

select
    m.employment_type,
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
    quarterly_change q on (m.month = q.month and m.employment_type = q.employment_type)
left join
    semiannual_change s on (m.month = s.month and m.employment_type = s.employment_type)
left join
    yearly_change y on (m.month = y.month and m.employment_type = y.employment_type)
join
    min_sample_size ms
    on (m.month = ms.month and m.employment_type = ms.employment_type)
order by m.employment_type, m.month
