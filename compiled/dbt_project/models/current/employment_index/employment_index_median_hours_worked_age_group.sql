

with
    combined_platform_aus as (
        select * from "dev"."employment_index"."eh_kp_combined_hours"
    ),
    age_group_binned as (
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
                when age < 18
                then 'Under 18'
                when age between 18 and 24
                then '18-24 year olds'
                when age between 25 and 64
                then '25-64 year olds'
                when age > 64
                then '65+ year olds'
            end as age_group
        from combined_platform_aus
    ),
    med_hours_worked as (
        select distinct
            age_group,
            month,
            median(monthly_hours) over (
                partition by age_group, month
            ) as median_hours_worked
        from age_group_binned
        where age_group is not null
        order by age_group, month
    ),
    monthly_change as (
        select
            age_group,
            month,
            median_hours_worked,
            lag(median_hours_worked) over (
                partition by age_group order by age_group, month
            ) as previous_month_lag,
            case
                when median_hours_worked = 0
                then 0
                else
                    round(
                        (median_hours_worked - previous_month_lag)
                        /  previous_month_lag,
                        3
                    )
            end as hours_worked_growth_monthly
        from med_hours_worked
        where age_group is not null
        order by age_group, month
    ),
    quarterly_change as (
        select
            age_group,
            month,
            median_hours_worked,
            lag(median_hours_worked, 3) over (
                partition by age_group order by age_group, month
            ) as previous_quarter_lag,
            round(
                (median_hours_worked - previous_quarter_lag) / previous_quarter_lag, 3
            ) as hours_worked_growth_quarterly
        from med_hours_worked
        where age_group is not null
        order by age_group, month
    ),
    semiannual_change as (
        select
            age_group,
            month,
            median_hours_worked,
            lag(median_hours_worked, 6) over (
                partition by age_group order by age_group, month
            ) as previous_semiannual_lag,
            round(
                (median_hours_worked - previous_semiannual_lag) / previous_semiannual_lag, 3
            ) as hours_worked_growth_semiannual
        from med_hours_worked
        where age_group is not null
        order by age_group, month
    ),
    yearly_change as (
        select
            age_group,
            month,
            median_hours_worked,
            lag(median_hours_worked, 12) over (
                partition by age_group order by age_group, month
            ) as previous_yearly_lag,
            round(
                (median_hours_worked - previous_yearly_lag) / previous_yearly_lag, 3
            ) as hours_worked_growth_yearly
        from med_hours_worked
        where age_group is not null
        order by age_group, month
    ),
    min_sample_size as (
        select
            age_group,
            month,
            count(distinct organisation_id) as business_sample,
            count(distinct member_id) as employee_sample
        from age_group_binned
        group by 1, 2
        having business_sample > 150
    )

select
    m.age_group,
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
left join quarterly_change q on (m.month = q.month and m.age_group = q.age_group)
left join semiannual_change s on (m.month = s.month and m.age_group = s.age_group)
left join yearly_change y on (m.month = y.month and m.age_group = y.age_group)
join min_sample_size ms on (m.month = ms.month and m.age_group = ms.age_group)
order by m.age_group, m.month