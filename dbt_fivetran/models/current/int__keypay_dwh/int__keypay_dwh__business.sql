{{ config(alias="business", materialized="incremental", unique_key="id", incremental_strategy="delete+insert", on_schema_change= "sync_all_columns") }}
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
from {{ ref("stg__keypay_dwh__business") }}
where
    1 = 1
    {% if is_incremental() %}

        -- this filter will only be applied on an incremental run
        -- (uses > to include records whose timestamp occurred since the last run of this model)
        and _transaction_date > (select max(_transaction_date) from {{ this }})

    {% endif %}
qualify row_number()
    over (partition by id order by _transaction_date desc)
= 1
