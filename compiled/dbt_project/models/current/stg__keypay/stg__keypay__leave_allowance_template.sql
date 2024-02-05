
with source as (

    select * from "dev"."keypay_s3"."leave_allowance_template"

),

renamed as (

    select
        id::bigint                                            as id,  -- noqa: RF04
        business_id::bigint                                   as business_id,
        name::varchar                                         as name,  -- noqa: RF04
        external_reference_id::bigint                         as external_reference_id,
        source::bigint                                        as source,  -- noqa: RF04
        business_award_package_id::bigint                     as business_award_package_id,
        leave_accrual_start_date_type::bigint                 as leave_accrual_start_date_type,
        leave_year_start::varchar                             as leave_year_start,
        leave_loading_calculated_from_pay_category_id::bigint as leave_loading_calculated_from_pay_category_id,
        _file::varchar                                        as _file,
        _transaction_date::date                               as _transaction_date,
        _etl_date::timestamp                                  as _etl_date,
        _modified::timestamp                                  as _modified
    from source

)

select * from renamed