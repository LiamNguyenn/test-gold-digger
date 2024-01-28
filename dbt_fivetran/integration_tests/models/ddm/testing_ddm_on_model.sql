{{ 
    config(materialized='table', post_hook = "{{ mask_model() }}") 
}}

select
    got.user_id,
    got.email,
    got.phone_number,
    expected.email        as expected_email,
    expected.phone_number as expected_phone_number
from {{ ref('data_mask_model') }} as got
left join {{ ref('data_mask_model_expected') }} as expected
    on got.user_id = expected.user_id
