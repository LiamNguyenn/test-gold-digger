select distinct
    md5(cast(coalesce(cast(cleansed_name as TEXT), '_dbt_utils_surrogate_key_null_') as TEXT)) as dim_service_sk,
    cleansed_name                                                                              as service_name
from "dev"."intermediate"."int_cleansed_service_names"
where
    cleansed_name is not NULL
    and cleansed_name != ''