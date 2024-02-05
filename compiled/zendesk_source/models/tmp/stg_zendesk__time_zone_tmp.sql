--To disable this model, set the using_schedules variable within your dbt_project.yml file to False.


select "time_zone",
  "standard_offset",
  "_fivetran_synced" 
from "dev"."zendesk"."time_zone" as time_zone_table