

with
    combined_pca as (
        select
            month,
            member_id,
            organisation_id,
            gender,
            industry,
            residential_state,
            employment_type,
            category,
            net_earnings,
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
        from "dev"."employment_index"."eh_pay_category" p
    ),
    age_group_net_earnings as (
        select distinct age_group, category, month,
        median(net_earnings) over (
            partition by age_group, category, month
        ) as monthly_net_earnings
        from combined_pca
        where age_group is not null and category is not null
        ---group by 1, 2, 3, 4
        order by age_group, category, month
    ),
    monthly_change as (
        select
            age_group,
            category,
            month,
            monthly_net_earnings,
            lag(monthly_net_earnings) over (
                partition by age_group, category order by age_group, category, month
            ) as previous_month_lag,
            case
                when monthly_net_earnings = 0 or previous_month_lag = 0 
                then 0
                else
                    round(
                        (monthly_net_earnings - previous_month_lag)
                        / previous_month_lag,
                        3
                    )
            end as net_earnings_growth_monthly
        from age_group_net_earnings
        order by age_group, category, month
    ),
    quarterly_change as (
    select
            age_group,
            category,
            month,
            monthly_net_earnings,
            lag(monthly_net_earnings, 3) over (
                partition by age_group, category order by age_group, category, month
            ) as previous_quarter_lag,
            case
                when monthly_net_earnings = 0 or previous_quarter_lag = 0 
                then 0
                else
                    round(
                        (monthly_net_earnings - previous_quarter_lag)
                        / previous_quarter_lag,
                        3
                    )
            end as net_earnings_growth_quarterly
        from age_group_net_earnings
        order by age_group, category, month
    ),
    semiannual_change as (
    select
            age_group,
            category,
            month,
            monthly_net_earnings,
            lag(monthly_net_earnings, 6) over (
                partition by age_group, category order by age_group, category, month
            ) as previous_semiannual_lag,
            case
                when monthly_net_earnings = 0 or previous_semiannual_lag = 0 
                then 0
                else
                    round(
                        (monthly_net_earnings - previous_semiannual_lag)
                        / previous_semiannual_lag,
                        3
                    )
            end as net_earnings_growth_semiannual
        from age_group_net_earnings
        order by age_group, category, month
    ),
    yearly_change as (
    select
            age_group,
            category,
            month,
            monthly_net_earnings,
            lag(monthly_net_earnings, 12) over (
                partition by age_group, category order by age_group, category, month
            ) as previous_yearly_lag,
            case
                when monthly_net_earnings = 0 or previous_yearly_lag = 0 
                then 0
                else
                    round(
                        (monthly_net_earnings - previous_yearly_lag)
                        / previous_yearly_lag,
                        3
                    )
            end as net_earnings_growth_yearly
        from age_group_net_earnings
        order by age_group, category, month
    ),
        min_sample_size as (
        select
            age_group,
            month,
            category,
            count(distinct organisation_id) as business_sample,
            count(distinct member_id) as employee_sample
        from combined_pca
        group by 1, 2, 3
        having business_sample > 150
    )
select
    m.age_group,
    m.category,
    m.month,
    m.monthly_net_earnings,
    previous_month_lag,
    net_earnings_growth_monthly,
    previous_quarter_lag,
    net_earnings_growth_quarterly,
    previous_semiannual_lag,
    net_earnings_growth_semiannual,
    previous_yearly_lag,
    net_earnings_growth_yearly
from monthly_change m
left join quarterly_change q on (m.month = q.month and m.age_group = q.age_group and m.category = q.category)
left join semiannual_change s on (m.month = s.month and m.age_group = s.age_group and m.category = s.category)
left join yearly_change y on (m.month = y.month and m.age_group = y.age_group and m.category = y.category)
--join min_sample_size ms on (m.month = ms.month and m.age_group = ms.age_group and m.category = ms.category)
order by m.age_group, m.category, m.month LIMIT 10000