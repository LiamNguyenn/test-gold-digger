{{ config(alias='leave_category', materialized = 'view') }}
with source as (

    select * from {{ source('keypay_s3', 'leave_category') }}

),

renamed as (

    select
        id::bigint                                as id,  -- noqa: RF04
        leave_category_name::varchar              as leave_category_name,
        business_id::bigint                       as business_id,
        exclude_from_termination_payout::boolean  as exclude_from_termination_payout,
        is_deleted::boolean                       as is_deleted,
        unit_type::bigint                         as unit_type,
        source::bigint                            as source,  -- noqa: RF04
        date_created::varchar                     as date_created,
        deduct_from_primary_pay_category::bigint  as deduct_from_primary_pay_category,
        deduct_from_pay_category_id::bigint       as deduct_from_pay_category_id,
        transfer_to_pay_category_id::bigint       as transfer_to_pay_category_id,
        leave_category_type::bigint               as leave_category_type,
        entitlement_period::double precision      as entitlement_period,
        contingent_period::double precision       as contingent_period,
        automatically_accrues::boolean            as automatically_accrues,
        standard_hours_per_year::double precision as standard_hours_per_year,
        units::float                              as units,
        _file::varchar                            as _file,
        _transaction_date::date                   as _transaction_date,
        _etl_date::timestamp                      as _etl_date,
        _modified::timestamp                      as _modified,
        is_balance_untracked::boolean             as is_balance_untracked
    from source

)

select * from renamed
