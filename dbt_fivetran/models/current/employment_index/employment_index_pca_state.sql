{{ config(alias="pca_state") }}

with
    business_organisation_overlap as (
        select distinct organisation_id, pr.business_id as kp_business_id
        from
            (
                select epa.organisation_id, external_id
                from {{ ref("employment_hero_v_last_connected_payroll") }} as epa
                join postgres_public.payroll_infos pi on payroll_info_id = pi.id
                where epa.type = 'KeypayAuth' and not pi._fivetran_deleted
            ) as o
        join
            keypay._t_pay_run_total_monthly_summary pr on pr.business_id = o.external_id
    ),

    combined_pca as (
        select *
        from {{ref('employment_index_eh_pay_category')}} p
    ),
    residential_state_net_earnings as (
        select distinct
            residential_state,
            category,
            month,
            median(net_earnings) over (
                partition by residential_state, category, month
            ) as monthly_net_earnings
        from combined_pca
        where residential_state is not null and category is not null
        order by residential_state, category, month
    ),
    monthly_change as (
        select
            residential_state,
            category,
            month,
            monthly_net_earnings,
            lag(monthly_net_earnings) over (
                partition by residential_state, category
                order by residential_state, category, month
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
        from residential_state_net_earnings
        order by residential_state, category, month
    ),
    quarterly_change as (
        select
            residential_state,
            category,
            month,
            monthly_net_earnings,
            lag(monthly_net_earnings, 3) over (
                partition by residential_state, category
                order by residential_state, category, month
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
        from residential_state_net_earnings
        order by residential_state, category, month
    ),
    semiannual_change as (
        select
            residential_state,
            category,
            month,
            monthly_net_earnings,
            lag(monthly_net_earnings, 6) over (
                partition by residential_state, category
                order by residential_state, category, month
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
        from residential_state_net_earnings
        order by residential_state, category, month
    ),
    yearly_change as (
        select
            residential_state,
            category,
            month,
            monthly_net_earnings,
            lag(monthly_net_earnings, 12) over (
                partition by residential_state, category
                order by residential_state, category, month
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
        from residential_state_net_earnings
        order by residential_state, category, month
    ),
    min_sample_size as (
        select
            residential_state,
            month,
            category,
            count(distinct organisation_id) as business_sample,
            count(distinct member_id) as employee_sample
        from combined_pca
        group by 1, 2, 3
        having business_sample > 150
    )
select
    m.residential_state,
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
left join
    quarterly_change q
    on (
        m.month = q.month
        and m.residential_state = q.residential_state
        and m.category = q.category
    )
left join
    semiannual_change s
    on (
        m.month = s.month
        and m.residential_state = s.residential_state
        and m.category = s.category
    )
left join
    yearly_change y
    on (
        m.month = y.month
        and m.residential_state = y.residential_state
        and m.category = y.category
    )
-- join min_sample_size ms on (m.month = ms.month and m.residential_state =
-- ms.residential_state and m.category = ms.category)
order by m.residential_state, m.category, m.month
