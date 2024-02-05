
with source as (

    select * from "dev"."keypay_s3"."super_details"

),

renamed as (

    select
        id::bigint                            as id,  -- noqa: RF04
        business_id::bigint                   as business_id,
        date_registered_utc::varchar          as date_registered_utc,
        enabled::boolean                      as enabled,
        date_beam_terms_accepted_utc::varchar as date_beam_terms_accepted_utc,
        _file::varchar                        as _file,
        _transaction_date::date               as _transaction_date,
        _etl_date::timestamp                  as _etl_date,
        _modified::timestamp                  as _modified
    from source

)

select * from renamed