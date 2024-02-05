--To disable this model, set the using_user_tags variable within your dbt_project.yml file to False.


select "tag",
  "user_id",
  "_fivetran_synced"  
from "dev"."zendesk"."user_tag" as user_tag_table