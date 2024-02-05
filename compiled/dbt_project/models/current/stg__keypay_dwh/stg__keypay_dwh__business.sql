
with source as (

    select * from "dev"."keypay_s3"."business"

),

renamed as (

    select
        id::bigint                                                      as id,  -- noqa: RF04
        name::varchar                                                   as name,  -- noqa: RF04
        abn::varchar                                                    as abn,
        legal_name::varchar                                             as legal_name,
        to_timestamp(date_created, 'MM/DD/YYYY hh/mi/ss tt')::timestamp as date_created,
        industry_id::bigint                                             as industry_id,
        industry_name::varchar                                          as industry_name,
        address_line1::varchar                                          as address_line1,
        address_line2::varchar                                          as address_line2,
        suburb_id::bigint                                               as suburb_id,
        billing_plan_id::bigint                                         as billing_plan_id,
        commence_billing_from::varchar                                  as commence_billing_from,
        decode(
            to_be_deleted,
            'false', 0,
            'true', 1,
            'False', 0,
            'True', 1
        )                                                               as to_be_deleted,

        white_label_id::bigint                                          as white_label_id,
        electronic_payroll_lodgement_enabled::boolean                   as electronic_payroll_lodgement_enabled,
        _file::varchar                                                  as _file,
        _transaction_date::date                                         as _transaction_date,
        _etl_date::timestamp                                            as _etl_date,
        _modified::timestamp                                            as _modified
    from source

)

select * from renamed