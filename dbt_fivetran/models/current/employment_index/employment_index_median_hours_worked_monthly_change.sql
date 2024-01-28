{{ config(alias="median_hours_worked_monthly_change") }}

with
    med_hours_worked as (
        select * from {{ ref("employment_index_median_hours_worked_aus") }}
    ),
    monthly_change as (
        select
            month,
            median_hours_worked,
            lag(median_hours_worked) over (order by month) as previous_month_lag,
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
        order by month
    ),
    quarterly_change as (
        select
            month,
            median_hours_worked,
            lag(median_hours_worked, 3) over (order by month) as previous_quarter_lag,
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
        order by month
    ),
    semiannual_change as (
        select
            month,
            median_hours_worked,
            lag(median_hours_worked, 6) over (
                order by month
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
        order by month
    ),
    yearly_change as (
        select
            month,
            median_hours_worked,
            lag(median_hours_worked, 12) over (order by month) as previous_yearly_lag,
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
        order by month
    )

select
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
left join quarterly_change q on m.month = q.month
left join semiannual_change s on m.month = s.month
left join yearly_change y on m.month = y.month
order by m.month
