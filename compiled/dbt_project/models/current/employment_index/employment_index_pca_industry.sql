

with
    combined_pca as (
        select *
        from "dev"."employment_index"."eh_pay_category" p
    ),
    industry_net_earnings as (
        select distinct industry, category, month, 
        median(net_earnings) over (
            partition by industry, category, month
        ) as monthly_net_earnings
        from combined_pca
        where industry is not null and category is not null
        order by industry, category, month
    ),
    monthly_change as (
        select
            industry,
            category,
            month,
            monthly_net_earnings,
            lag(monthly_net_earnings) over (
                partition by industry, category order by industry, category, month
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
        from industry_net_earnings
        order by industry, category, month
    ),
    quarterly_change as (
    select
            industry,
            category,
            month,
            monthly_net_earnings,
            lag(monthly_net_earnings, 3) over (
                partition by industry, category order by industry, category, month
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
        from industry_net_earnings
        order by industry, category, month
    ),
    semiannual_change as (
    select
            industry,
            category,
            month,
            monthly_net_earnings,
            lag(monthly_net_earnings, 6) over (
                partition by industry, category order by industry, category, month
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
        from industry_net_earnings
        order by industry, category, month
    ),
    yearly_change as (
    select
            industry,
            category,
            month,
            monthly_net_earnings,
            lag(monthly_net_earnings, 12) over (
                partition by industry, category order by industry, category, month
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
        from industry_net_earnings
        order by industry, category, month
    ),
        min_sample_size as (
        select
            industry,
            month,
            category,
            count(distinct organisation_id) as business_sample,
            count(distinct member_id) as employee_sample
        from combined_pca
        group by 1, 2, 3
        having business_sample > 150
    )
select
    m.industry,
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
left join quarterly_change q on (m.month = q.month and m.industry = q.industry and m.category = q.category)
left join semiannual_change s on (m.month = s.month and m.industry = s.industry and m.category = s.category)
left join yearly_change y on (m.month = y.month and m.industry = y.industry and m.category = y.category)
--join min_sample_size ms on (m.month = ms.month and m.industry = ms.industry and m.category = ms.category)
order by m.industry, m.category, m.month