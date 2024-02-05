--To disable this model, set the using_schedules variable within your dbt_project.yml file to False.




select "created_at",
  "ticket_id",
  "schedule_id",
  "_fivetran_synced"
from "dev"."zendesk"."ticket_schedule" as ticket_schedule_table

