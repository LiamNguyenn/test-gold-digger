
    with source as (

select * from "dev"."mart__keypay"."business_traits"

),

renamed as (

select
            "id",
            "name",
            "created_at",
            "industry_id",
            "industry_name",
            "country",
            "commence_billing_from",
            "white_label_id",
            "white_label_name",
            "partner_id",
            "partner_name"
from source

)

select *
from renamed