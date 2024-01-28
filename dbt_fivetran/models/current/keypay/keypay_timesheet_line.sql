{{ config(alias='timesheet_line', materialized = 'view') }}
    with source as (

select * from {{ ref('int__keypay__timesheet_line') }}

),

renamed as (

select
            "_file",
            "_modified",
            "_transaction_date",
            "_etl_date",
            "id",
            "employee_id",
            "start_time",
            "end_time",
            "units",
            "date_created",
            "submitted_start_time",
            "submitted_end_time",
            "pay_category_id",
            "status",
            "leave_request_id",
            "consolidated_with_timesheet_line_id",
            "pay_run_total_id",
            "business_id",
            "auto_approved_by_roster_shift_id"
from source

)

select *
from renamed
