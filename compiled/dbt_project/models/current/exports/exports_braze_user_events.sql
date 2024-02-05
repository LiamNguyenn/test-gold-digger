

select *
from "dev"."exports"."exports_v_braze_user_events_tmp"
where
    1 = 1
    
        and event_time > (select max(event_time) from "dev"."exports"."exports_braze_user_events")
    
qualify row_number() over (partition by event_id order by event_time desc) = 1