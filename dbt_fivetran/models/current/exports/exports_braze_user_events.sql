{{ 
    config(
        materialized='incremental',
        on_schema_change='append_new_columns',
        dist_key='user_uuid',
        sort_key='event_time',
    )
}}

select *
from {{ ref('exports_v_braze_user_events_tmp') }}
where
    1 = 1
    {% if is_incremental() %}
        and event_time > (select max(event_time) from {{ this }})
    {% endif %}
qualify row_number() over (partition by event_id order by event_time desc) = 1
