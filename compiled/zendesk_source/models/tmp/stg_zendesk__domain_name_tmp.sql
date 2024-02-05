--To disable this model, set the using_domain_names variable within your dbt_project.yml file to False.


select "organization_id",
  "index",
  "domain_name",
  "_fivetran_synced" 
from "dev"."zendesk"."domain_name" as domain_name_table