with source as (
    select *

    from "dev"."postgres_public"."members"
),

transformed as (
    select
        id::int                        as id,  --noqa: RF04
        uuid::varchar                  as uuid,
        external_id::int               as external_payroll_employee_id,
        first_name::varchar            as first_name,
        middle_name::varchar           as middle_name,
        last_name::varchar             as last_name,
        organisation_id::int           as organisation_id,
        accepted::boolean              as has_accepted_invitation,
        gender::varchar                as gender,
        date_part(year, date_of_birth) as birth_year,
        start_date::varchar            as start_date,
        termination_date::varchar      as termination_date,
        active::boolean                as is_active,
        work_country,
        user_id,
        role_id,
        created_at::timestamp          as created_at,
        _fivetran_synced::timestamp    as fivetran_synced


    from source

    where
        not _fivetran_deleted
        and not is_shadow_data
)

select * from transformed