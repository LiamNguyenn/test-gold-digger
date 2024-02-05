--To disable this model, set the using_ticket_form_history variable within your dbt_project.yml file to False.


select "id",
  "updated_at",
  "created_at",
  "name",
  "display_name",
  "end_user_visible",
  "active",
  "_fivetran_deleted",
  "_fivetran_synced"
from "dev"."zendesk"."ticket_form_history" as ticket_form_history_table