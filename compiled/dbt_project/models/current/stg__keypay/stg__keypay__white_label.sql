
with source as (

    select * from "dev"."keypay_s3"."white_label"

),

renamed as (

    select
        id::bigint                                        as id,  -- noqa: RF04
        name::varchar                                     as name,  -- noqa: RF04
        is_deleted::boolean                               as is_deleted,
        region_id::bigint                                 as region_id,
        support_email::varchar                            as support_email,
        primary_champion_id::bigint                       as primary_champion_id,
        function_enable_super_choice_marketplace::boolean as function_enable_super_choice_marketplace,
        default_billing_plan_id::bigint                   as default_billing_plan_id,
        reseller_id::bigint                               as reseller_id,
        _file::varchar                                    as _file,
        _transaction_date::date                           as _transaction_date,
        _etl_date::timestamp                              as _etl_date,
        _modified::timestamp                              as _modified
    from source

)

select * from renamed