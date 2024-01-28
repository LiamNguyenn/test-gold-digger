select distinct
    {{ dbt_utils.generate_surrogate_key(['cleansed_name']) }} as dim_service_sk,
    cleansed_name                                                                              as service_name
from {{ ref("int_cleansed_service_names") }}
where
    cleansed_name is not NULL
    and cleansed_name != ''
