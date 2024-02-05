with eh_users as (
    select *

    from "dev"."intermediate"."int_enrich_eh_users"
),

keypay_users as (
    select *

    from "dev"."intermediate"."int_enrich_keypay_users"
)

select
    'employment_hero'                                                                                                                                                                                                                                         as platform,
    eh_user_id,
    eh_user_uuid,
    NULL                                                                                                                                                                                                                                                      as keypay_user_id,
    md5(cast(coalesce(cast(platform as TEXT), '_dbt_utils_surrogate_key_null_') || '-' || coalesce(cast(eh_user_uuid as TEXT), '_dbt_utils_surrogate_key_null_') || '-' || coalesce(cast(keypay_user_id as TEXT), '_dbt_utils_surrogate_key_null_') as TEXT)) as dim_user_sk,
    first_name,
    last_name,
    email,
    has_acknowledged_eh_tnc,
    is_twofa_enabled,
    is_verified,
    is_profile_completed,
    is_public_profile,
    is_active,
    is_current_eh_employee,
    is_active_employee,
    active_employee_count,
    terminated_employee_count,
    case
        when (is_active_employee or is_current_eh_employee) then 'employee'
        else 'candidate'
    end                                                                                                                                                                                                                                                       as current_persona,
    NULL                                                                                                                                                                                                                                                      as is_payroll_admin,
    is_marketing_consented,
    marketing_consented_at,
    has_swag_profile,
    created_at

from eh_users

union distinct

select
    'keypay'                                                                                                                                                                                                                                                  as platform,
    NULL                                                                                                                                                                                                                                                      as eh_user_id,
    NULL                                                                                                                                                                                                                                                      as eh_user_uuid,
    keypay_user_id,
    md5(cast(coalesce(cast(platform as TEXT), '_dbt_utils_surrogate_key_null_') || '-' || coalesce(cast(eh_user_uuid as TEXT), '_dbt_utils_surrogate_key_null_') || '-' || coalesce(cast(keypay_user_id as TEXT), '_dbt_utils_surrogate_key_null_') as TEXT)) as dim_user_sk,
    first_name,
    last_name,
    email,
    NULL                                                                                                                                                                                                                                                      as has_acknowledged_eh_tnc,
    NULL                                                                                                                                                                                                                                                      as is_twofa_enabled,
    is_active                                                                                                                                                                                                                                                 as is_verified,
    NULL                                                                                                                                                                                                                                                      as is_profile_completed,
    NULL                                                                                                                                                                                                                                                      as is_public_profile,
    is_active,
    is_current_eh_employee,
    is_active_employee,
    active_employee_count,
    terminated_employee_count,
    case
        when (is_active_employee or is_current_eh_employee) then 'employee'
        else 'candidate'
    end                                                                                                                                                                                                                                                       as current_persona,
    is_admin                                                                                                                                                                                                                                                  as is_payroll_admin,
    NULL                                                                                                                                                                                                                                                      as is_marketing_consented,
    NULL                                                                                                                                                                                                                                                      as marketing_consented_at,
    has_swag_profile,
    NULL                                                                                                                                                                                                                                                      as created_at

from keypay_users