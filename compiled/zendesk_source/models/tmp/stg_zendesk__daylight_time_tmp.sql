--To disable this model, set the using_schedules variable within your dbt_project.yml file to False.


select "year",
  "time_zone",
  "daylight_start_utc",
  "daylight_end_utc",
  "daylight_offset",
  "_fivetran_synced"
from "dev"."zendesk"."daylight_time" as daylight_time_table