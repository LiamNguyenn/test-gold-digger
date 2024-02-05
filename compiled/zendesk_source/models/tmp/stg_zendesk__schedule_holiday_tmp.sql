--To disable this model, set the using_schedules variable within your dbt_project.yml file to False.


select "schedule_id",
  "id",
  "_fivetran_deleted",
  "_fivetran_synced"
from "dev"."zendesk"."schedule_holiday" as schedule_holiday_table