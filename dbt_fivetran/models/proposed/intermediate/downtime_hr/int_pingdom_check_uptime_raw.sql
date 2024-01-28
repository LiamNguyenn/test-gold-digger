select
    wppcs.id           as pingdom_check_stats_id,
    wppc.id            as pingdom_check_id,
    srvc.cleansed_name as service_name,
    wppcs.date,
    wppcs.up_time
from {{ ref("stg_workshop_public__pingdom_check_stats") }} as wppcs
left join {{ ref("stg_workshop_public__pingdom_checks") }} as wppc on wppcs.pingdom_check_id = wppc.id
inner join {{ ref("int_cleansed_service_names") }} as srvc on wppc.name = srvc.name
left join {{ ref("stg_eh_engineering__service_ownership") }} as eeso on srvc.cleansed_name = eeso.service
where
    eeso.nonapplicable is NULL
    and wppcs.unknown_time not like 86399 --this is to exlcude cases where no information is known about uptime/downtime 
