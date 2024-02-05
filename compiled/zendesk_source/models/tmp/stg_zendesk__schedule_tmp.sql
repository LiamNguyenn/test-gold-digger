--To disable this model, set the using_schedules variable within your dbt_project.yml file to False.


select "id",
  "name",
  "time_zone",
  "start_time",
  "end_time",
  "_fivetran_deleted",
  "_fivetran_synced",
  "end_time_utc",
  "start_time_utc",
  "created_at"
from "dev"."zendesk"."schedule" as schedule_table