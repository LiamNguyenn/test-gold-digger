{{ config(alias="pca_monthly_change") }}

with
    pca_aus as (
        select * from {{ ref("employment_index_pca_aus") }}
    ),
 monthly_change as (
        select
            category,
            month,
            monthly_net_earnings,
            lag(monthly_net_earnings) over (
                partition by category order by category, month
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
        from pca_aus
        order by category, month
    ),
    quarterly_change as (
        select
            category,
            month,
            monthly_net_earnings,
            lag(monthly_net_earnings, 3) over (
                partition by category order by category, month
            ) as previous_quarterly_lag,
            case
                when monthly_net_earnings = 0 or previous_quarterly_lag = 0 
                then 0
                else
                    round(
                        (monthly_net_earnings - previous_quarterly_lag)
                        / previous_quarterly_lag,
                        3
                    )
            end as net_earnings_growth_quarterly
        from pca_aus
        order by category, month
    ),
        semiannual_change as (
        select
            category,
            month,
            monthly_net_earnings,
            lag(monthly_net_earnings, 6) over (
                partition by category order by category, month
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
        from pca_aus
        order by category, month
    ),
        yearly_change as (
        select
            category,
            month,
            monthly_net_earnings,
            lag(monthly_net_earnings, 12) over (
                partition by category order by category, month
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
        from pca_aus
        order by category, month
    )

select
    m.category,
    m.month,
    m.monthly_net_earnings,
    previous_month_lag,
    net_earnings_growth_monthly,
    previous_quarterly_lag,
    net_earnings_growth_quarterly,
    previous_semiannual_lag,
    net_earnings_growth_semiannual,
    previous_yearly_lag,
    net_earnings_growth_yearly
from monthly_change m
left join quarterly_change q on (m.month = q.month and m.category = q.category)
left join semiannual_change s on (m.month = s.month and m.category = s.category)
left join yearly_change y on (m.month = y.month and m.category = y.category)
order by m.category, m.month