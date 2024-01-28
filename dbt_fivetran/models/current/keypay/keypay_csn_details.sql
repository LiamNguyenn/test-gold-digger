{{ config(alias='csn_details', materialized = 'view') }}
    with source as (

select * from {{ ref('int__keypay__csn_details') }}

),

renamed as (

select
            "id",
            "business_id",
            "cpf_submission_number",
            "csn_type",
            "is_deleted",
            "_transaction_date",
            "_etl_date",
            "_modified",
            "_file"
from source

)

select *
from renamed
