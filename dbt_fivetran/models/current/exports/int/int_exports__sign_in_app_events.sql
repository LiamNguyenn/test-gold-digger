{{ config(materialized='view', schema='exports') }}
with eben_users as (
  select
    (
      case
        when
          json_extract_path_text(detail, 'ehUUId') = ''
          then null
        else json_extract_path_text(detail, 'ehUUId')
      end
    )         as eh_user_uuid,
    max(time) as event_time
  from {{ source('ebenefits', 'user_created') }}
  group by 1
),

latest_devices as (
  select distinct
    user_uuid,
    first_value(device)
      ignore nulls over (
        partition by user_uuid
        order by
          "timestamp" desc
        rows between unbounded preceding and unbounded following
      )
    as device
  from
    {{ ref('customers_int_events') }}
  where
    device is not null
),

final as (
  select
    u1.eh_user_uuid                        as user_uuid,
    cast('first_signed_in_app' as varchar) as event_name,
    u1.event_time,
    device
  from eben_users as u1
  inner join {{ ref('exports_braze_users') }} as u2
    on u1.eh_user_uuid = u2.user_uuid
  left join latest_devices as d
    on u1.eh_user_uuid = d.user_uuid
)

select
  {{ dbt_utils.generate_surrogate_key(['user_uuid', 'event_name', 'event_time']) }} as event_id,
  *
from final
