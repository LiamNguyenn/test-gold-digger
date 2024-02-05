

with
    combined_platform_aus as (
        select * from "dev"."employment_index"."eh_kp_combined_hours"
    ),
    company_size_binned as (
        select
            month,
            member_id,
            organisation_id,
            gender,
            industry,
            residential_state,
            employment_type,
            monthly_hours,
            age,
            case
                when total_employees < 20
                then '1-19'
                when total_employees between 19 and 200
                then '20-199'
                when total_employees > 200
                then '200+'
            end as company_size
        from combined_platform_aus
    ),
    med_hours_worked as (
        select distinct
            company_size,
            month,
            median(monthly_hours) over (
                partition by company_size, month
            ) as median_hours_worked
        from company_size_binned
        where company_size is not null
        order by company_size, month
    ),
    monthly_change as (
        select
            company_size,
            month,
            median_hours_worked,
            lag(median_hours_worked) over (
                partition by company_size order by company_size, month
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
        order by company_size, month
    ),
    quarterly_change as (
        select
            company_size,
            month,
            median_hours_worked,
            lag(median_hours_worked, 3) over (
                partition by company_size order by company_size, month
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
        order by company_size, month
    ),
    semiannual_change as (
        select
            company_size,
            month,
            median_hours_worked,
            lag(median_hours_worked, 6) over (
                partition by company_size order by company_size, month
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
        order by company_size, month
    ),
    yearly_change as (
        select
            company_size,
            month,
            median_hours_worked,
            lag(median_hours_worked, 12) over (
                partition by company_size order by company_size, month
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
        order by company_size, month
    ),
    min_sample_size as (
        select
            company_size,
            month,
            count(distinct organisation_id) as business_sample,
            count(distinct member_id) as employee_sample
        from company_size_binned
        group by 1, 2
        having business_sample > 150
    )

select
    m.company_size,
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
left join quarterly_change q on (m.month = q.month and m.company_size = q.company_size)
left join semiannual_change s on (m.month = s.month and m.company_size = s.company_size)
left join yearly_change y on (m.month = y.month and m.company_size = y.company_size)
join min_sample_size ms on (m.month = ms.month and m.company_size = ms.company_size)
order by m.company_size, m.month