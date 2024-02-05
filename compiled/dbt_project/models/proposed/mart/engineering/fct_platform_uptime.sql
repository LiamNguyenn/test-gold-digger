with downtime_payroll as (
    select *

    from "dev"."intermediate"."int_checkly_daily_downtime"
),

downtime_hr as (
    select
        date,
        up_time
    from "dev"."intermediate"."int_pingdom_check_uptime_raw"
)

select
    date,
    

  to_number(to_char(date::DATE,'YYYYMMDD'),'99999999')

 as dim_date_sk,
    'Payroll'                                          as platform,
    86400 - sum(total_net_downtime)                    as overall_platform_uptime,
    86400 - sum(qbo_au_net_downtime)                   as qbo_au_platform_uptime,
    86400 - sum(qbo_uk_net_downtime)                   as qbo_uk_platform_uptime

from downtime_payroll

group by 1, 2, 3


union distinct
select
    date,
    

  to_number(to_char(date::DATE,'YYYYMMDD'),'99999999')

 as dim_date_sk,
    'HR'                                               as platform,
    avg(up_time)                                       as overall_platform_uptime,
    NULL                                               as qbo_au_platform_uptime,
    NULL                                               as qbo_uk_platform_uptime
from downtime_hr
group by
    1, 2, 3