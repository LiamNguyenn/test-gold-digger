with cleansed_service_name as (
    select
        dim_service_sk,
        service_name
    from "dev"."mart"."dim_service"
),

downtime_payroll as (
    select
        checkly.date,
        checkly.total_downtime,
        srvc.dim_service_sk
    from "dev"."intermediate"."int_checkly_daily_downtime" as checkly
    left join cleansed_service_name as srvc on checkly.name = srvc.service_name

),


downtime_hr as (
    select
        pingdom.date,
        pingdom.up_time,
        srvc.dim_service_sk
    from "dev"."intermediate"."int_pingdom_check_uptime_raw" as pingdom
    left join cleansed_service_name as srvc on pingdom.service_name = srvc.service_name

)

select
    date,
    

  to_number(to_char(date::DATE,'YYYYMMDD'),'99999999')

 as dim_date_sk,
    'Payroll'                                          as platform,
    dim_service_sk,
    86400 - sum(total_downtime)                        as service_uptime
from downtime_payroll

group by 1, 2, 3, 4
union distinct
select
    date,
    

  to_number(to_char(date::DATE,'YYYYMMDD'),'99999999')

 as dim_date_sk,
    'HR'                                               as platform,
    dim_service_sk,
    avg(up_time)                                       as service_uptime
from downtime_hr
group by
    1, 2, 3, 4