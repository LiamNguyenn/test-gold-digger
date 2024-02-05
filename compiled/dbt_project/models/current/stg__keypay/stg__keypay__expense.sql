
with source as (

    select * from "dev"."keypay_s3"."expense"

),

renamed as (

    select
        id::bigint                  as id,  -- noqa: RF04
        expense_date::varchar       as expense_date,
        business_id::bigint         as business_id,
        unit_cost::float            as unit_cost,
        quantity::float             as quantity,
        invoice_id::bigint          as invoice_id,
        notes::varchar              as notes,
        expense_type::bigint        as expense_type,
        displayed_unit_cost::bigint as displayed_unit_cost,
        _file::varchar              as _file,
        _transaction_date::date     as _transaction_date,
        _etl_date::timestamp        as _etl_date,
        _modified::timestamp        as _modified
    from source

)

select * from renamed