
    with source as (

select * from "dev"."int__keypay"."payrun"

),

renamed as (

select
            "id",
            "date_finalised",
            "pay_period_starting",
            "pay_period_ending",
            "date_paid",
            "business_id",
            "invoice_id",
            "date_first_finalised",
            "pay_run_lodgement_data_id",
            "notification_date",
            "finalised_by_id",
            "pay_cycle_id",
            "pay_cycle_frequency_id",
            "date_created_utc",
            "_transaction_date",
            "_etl_date",
            "_modified",
            "_file"
from source

)

select *
from renamed