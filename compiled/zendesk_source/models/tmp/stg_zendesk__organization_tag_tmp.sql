--To disable this model, set the using_organization_tags variable within your dbt_project.yml file to False.


select "tag",
  "organization_id",
  "_fivetran_synced"  
from "dev"."zendesk"."organization_tag" as organization_tag_table