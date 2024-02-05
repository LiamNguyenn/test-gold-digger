select "ticket_id",
  "user_id",
  "updated",
  "field_name",
  "_fivetran_synced",
  "value"
from "dev"."zendesk"."ticket_field_history" as ticket_field_history_table