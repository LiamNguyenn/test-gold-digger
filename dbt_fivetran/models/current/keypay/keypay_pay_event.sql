{{ config(alias='pay_event', materialized = 'view') }}
    with source as (

select * from {{ ref('int__keypay__pay_event') }}

),

renamed as (

select
            "id",
            "business_id",
            "date_created_utc",
            "status",
            "pay_run_id",
            "date_lodged_utc",
            "date_response_received_utc",
            "pay_run_lodgement_data_id",
            "is_deleted",
            "stp_version",
            "_transaction_date",
            "_etl_date",
            "_modified",
            "_file"
from source

)

select *
from renamed
