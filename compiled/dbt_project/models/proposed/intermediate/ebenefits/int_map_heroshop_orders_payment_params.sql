select
    id                                              as heroshop_order_id,
    json_extract_path_text(payment_params, 'nonce') as nonce,
    json_extract_path_text(
        replace(
            json_extract_path_text(payment_params, 'device_data'), '\\', ''
        ),
        'device_session_id',
        TRUE
    )                                               as device_session_id,
    json_extract_path_text(
        replace(
            json_extract_path_text(payment_params, 'device_data'), '\\', ''
        ),
        'fraud_merchant_id',
        TRUE
    )                                               as fraud_merchant_id,
    json_extract_path_text(
        replace(
            json_extract_path_text(payment_params, 'device_data'), '\\', ''
        ),
        'correlation_id',
        TRUE
    )                                               as correlation_id
from "dev"."staging"."stg_heroshop_db_public__orders"