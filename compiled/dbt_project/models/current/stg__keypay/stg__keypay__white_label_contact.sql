
with source as (

    select * from "dev"."keypay_s3"."white_label_contact"

),

renamed as (

    select
        id::bigint              as id, -- noqa: RF04
        white_label_id::bigint  as white_label_id,
        user_id::bigint         as user_id,
        contact_type::bigint    as contact_type,
        name::varchar           as name, -- noqa: RF04
        email::varchar          as email,
        _file::varchar          as _file,
        _transaction_date::date as _transaction_date,
        _etl_date::timestamp    as _etl_date,
        _modified::timestamp    as _modified
    from source

)

select * from renamed