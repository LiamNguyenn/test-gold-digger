with eh_users as (
    select *

    from {{ ref("int_enrich_eh_users") }}
),

keypay_users as (
    select *

    from {{ ref("int_enrich_keypay_users") }}
),

ebenefits_users as (
    select
        ebenefits.ebenefits_user_uuid,
        ebenefits.eh_user_uuid             as eben_eh_user_uuid,
        ebenefits.keypay_user_id           as eben_keypay_user_id,
        ebenefits.email                    as eben_email,
        {{ dbt_utils.star(from=ref("int_enrich_eh_users"), relation_alias="eh_users", prefix="hr_") }},
        {{ dbt_utils.star(from=ref("int_enrich_keypay_users"), relation_alias="keypay_users", prefix="payroll_") }}

    from {{ ref("stg_ebenefits__user_created") }} as ebenefits

    left join eh_users
        on ebenefits.eh_user_uuid = eh_users.eh_user_uuid

    left join keypay_users
        on ebenefits.keypay_user_id = keypay_users.keypay_user_id
)

select
    'employment_hero'                                                                                                                                                                                                                                                                                                                                                          as platform,
    eh_user_id                                                                                                                                                                                                                                                                                                                                                                 as dim_user_eh_user_id,
    eh_user_uuid                                                                                                                                                                                                                                                                                                                                                               as dim_user_eh_user_uuid,
    NULL                                                                                                                                                                                                                                                                                                                                                                       as dim_user_keypay_user_id,
    NULL                                                                                                                                                                                                                                                                                                                                                                       as dim_user_ebenefits_user_uuid,
    {{ dbt_utils.generate_surrogate_key(["platform", "dim_user_eh_user_id", "dim_user_keypay_user_id", "dim_user_ebenefits_user_uuid"]) }} as dim_user_sk,
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
    end                                                                                                                                                                                                                                                                                                                                                                        as current_persona,
    NULL                                                                                                                                                                                                                                                                                                                                                                       as is_payroll_admin,
    is_marketing_consented,
    marketing_consented_at,
    created_at

from eh_users

union distinct

select
    'keypay'                                                                                                                                                                                                                                                                                                                                                                   as platform,
    NULL                                                                                                                                                                                                                                                                                                                                                                       as dim_user_eh_user_id,
    NULL                                                                                                                                                                                                                                                                                                                                                                       as dim_user_eh_user_uuid,
    keypay_user_id                                                                                                                                                                                                                                                                                                                                                             as dim_user_keypay_user_id,
    NULL                                                                                                                                                                                                                                                                                                                                                                       as dim_user_ebenefits_user_uuid,
    {{ dbt_utils.generate_surrogate_key(["platform", "dim_user_eh_user_id", "dim_user_keypay_user_id", "dim_user_ebenefits_user_uuid"]) }} as dim_user_sk,
    first_name,
    last_name,
    email,
    NULL                                                                                                                                                                                                                                                                                                                                                                       as has_acknowledged_eh_tnc,
    NULL                                                                                                                                                                                                                                                                                                                                                                       as is_twofa_enabled,
    is_active                                                                                                                                                                                                                                                                                                                                                                  as is_verified,
    NULL                                                                                                                                                                                                                                                                                                                                                                       as is_profile_completed,
    NULL                                                                                                                                                                                                                                                                                                                                                                       as is_public_profile,
    is_active,
    is_current_eh_employee,
    is_active_employee,
    active_employee_count,
    terminated_employee_count,
    case
        when (is_active_employee or is_current_eh_employee) then 'employee'
        else 'candidate'
    end                                                                                                                                                                                                                                                                                                                                                                        as current_persona,
    is_admin                                                                                                                                                                                                                                                                                                                                                                   as is_payroll_admin,
    NULL                                                                                                                                                                                                                                                                                                                                                                       as is_marketing_consented,
    NULL                                                                                                                                                                                                                                                                                                                                                                       as marketing_consented_at,
    NULL                                                                                                                                                                                                                                                                                                                                                                       as created_at

from keypay_users

union distinct

select
    'ebenefits'                                                                                                                                                                                                                                                                                                                                                                as platform,
    hr_eh_user_id                                                                                                                                                                                                                                                                                                                                                              as dim_user_eh_user_id,
    hr_eh_user_uuid                                                                                                                                                                                                                                                                                                                                                            as dim_user_eh_user_uuid,
    payroll_keypay_user_id                                                                                                                                                                                                                                                                                                                                                     as dim_user_keypay_user_id,
    ebenefits_user_uuid                                                                                                                                                                                                                                                                                                                                                        as dim_user_ebenefits_user_uuid,
    {{ dbt_utils.generate_surrogate_key(["platform", "dim_user_eh_user_id", "dim_user_keypay_user_id", "dim_user_ebenefits_user_uuid"]) }} as dim_user_sk,
    coalesce(hr_first_name, payroll_last_name)                                                                                                                                                                                                                                                                                                                                 as first_name,
    coalesce(hr_last_name, payroll_last_name)                                                                                                                                                                                                                                                                                                                                  as last_name,
    eben_email                                                                                                                                                                                                                                                                                                                                                                 as email,
    hr_has_acknowledged_eh_tnc                                                                                                                                                                                                                                                                                                                                                 as has_acknowledged_eh_tnc,
    hr_is_twofa_enabled                                                                                                                                                                                                                                                                                                                                                        as is_twofa_enabled,
    hr_is_verified                                                                                                                                                                                                                                                                                                                                                             as is_verified,
    hr_is_profile_completed                                                                                                                                                                                                                                                                                                                                                    as is_profile_completed,
    hr_is_public_profile                                                                                                                                                                                                                                                                                                                                                       as is_public_profile,
    coalesce(hr_is_active, payroll_is_active)                                                                                                                                                                                                                                                                                                                                  as is_active,
    coalesce(hr_is_current_eh_employee, payroll_is_current_eh_employee)                                                                                                                                                                                                                                                                                                        as is_current_eh_employee,
    coalesce(hr_is_active_employee, payroll_is_active_employee)                                                                                                                                                                                                                                                                                                                as is_active_employee,
    coalesce(hr_active_employee_count, payroll_active_employee_count)                                                                                                                                                                                                                                                                                                          as active_employee_count,
    coalesce(hr_terminated_employee_count, payroll_terminated_employee_count)                                                                                                                                                                                                                                                                                                  as terminated_employee_count,
    case
        when (coalesce(hr_is_active_employee, payroll_is_active_employee) or coalesce(hr_is_current_eh_employee, payroll_is_current_eh_employee)) then 'employee'
        else 'candidate'
    end                                                                                                                                                                                                                                                                                                                                                                        as current_persona,
    payroll_is_admin                                                                                                                                                                                                                                                                                                                                                           as is_payroll_admin,
    hr_is_marketing_consented,
    hr_marketing_consented_at,
    hr_created_at

from ebenefits_users
