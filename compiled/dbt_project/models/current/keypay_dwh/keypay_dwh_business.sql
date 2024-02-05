
with source as (

    select * from "dev"."int__keypay_dwh"."business"

),

renamed as (

    select
        id,
        name,
        abn,
        legal_name,
        date_created,
        industry_id,
        industry_name,
        address_line1,
        address_line2,
        suburb_id,
        billing_plan_id,
        commence_billing_from,
        to_be_deleted,
        white_label_id,
        electronic_payroll_lodgement_enabled,
        _transaction_date,
        _etl_date,
        _modified,
        _file
    from source

)

select *
from renamed