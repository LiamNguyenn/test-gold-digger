with dates as (
    select date_day::date as report_date

    from {{ ref("stg_dates__date_spine") }}
)

select
    report_date,
    extract(year from report_date)                                                         as calendar_year,
    case when extract(month from report_date) > 6 then 2 else 1 end                        as calendar_year_half,
    extract(quarter from report_date)                                                      as calendar_quarter,
    extract(month from report_date)                                                        as calendar_month,
    extract(week from report_date)                                                         as calendar_week,
    extract(day from report_date)                                                          as calendar_day,
    to_char(report_date, 'Mon')                                                            as month_name,
    to_char(report_date, 'Dy')                                                             as day_name,
    case when calendar_year_half = 2 then calendar_year + 1 else calendar_year end         as financial_year,
    case when calendar_year_half = 2 then 1 else 2 end                                     as financial_year_half,
    case when calendar_year_half = 2 then calendar_quarter - 2 else calendar_month + 2 end as financial_quarter,
    case when calendar_year_half = 2 then calendar_month - 6 else calendar_month + 6 end   as financial_month

from dates
