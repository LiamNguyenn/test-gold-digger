with users as (
    select
        "id",
  "uuid",
  "email",
  "has_acknowledged_eh_tnc",
  "is_twofa_enabled",
  "created_at"

    from "dev"."staging"."stg_postgres_public__users"
),

user_infos as (
    select
        "user_id",
  "first_name",
  "last_name",
  "verified_at",
  "activated_at",
  "marketing_consented_at",
  "is_profile_completed",
  "is_public_profile"

    from "dev"."staging"."stg_postgres_public__user_infos"
),

members as (
    select
        "id",
  "uuid",
  "external_payroll_employee_id",
  "first_name",
  "middle_name",
  "last_name",
  "organisation_id",
  "has_accepted_invitation",
  "gender",
  "birth_year",
  "start_date",
  "termination_date",
  "is_active",
  "work_country",
  "user_id",
  "role_id",
  "created_at",
  "fivetran_synced"

    from "dev"."staging"."stg_postgres_public__members"
),

ebenefits_users as (
    select ebenefits.eh_user_uuid

    from "dev"."staging"."stg_ebenefits__user_created" as ebenefits

    group by 1
),

combined as (
    select
        users.id                                                                                                                                                                                 as eh_user_id,
        users.uuid                                                                                                                                                                               as eh_user_uuid,
        user_infos.first_name                                                                                                                                                                    as first_name,
        user_infos.last_name                                                                                                                                                                     as last_name,
        users.email                                                                                                                                                                              as email,
        users.has_acknowledged_eh_tnc,
        users.is_twofa_enabled,
        user_infos.verified_at is not NULL                                                                                                                                                       as is_verified,
        user_infos.verified_at,
        user_infos.is_profile_completed,
        user_infos.is_public_profile,
        user_infos.activated_at is not NULL                                                                                                                                                      as is_active,
        user_infos.activated_at,
        user_infos.marketing_consented_at is not NULL                                                                                                                                            as is_marketing_consented,
        user_infos.marketing_consented_at,
        ebenefits_users.eh_user_uuid is not NULL                                                                                                                                                 as has_swag_profile,
        users.created_at,
        count(distinct case when members.is_active and (members.termination_date > current_date or members.termination_date is NULL) and members.organisation_id = 8701 then members.id end) > 0 as is_current_eh_employee,
        count(distinct case when members.is_active and (members.termination_date > current_date or members.termination_date is NULL) then members.id end) > 0                                    as is_active_employee,
        count(distinct case when members.is_active and (members.termination_date > current_date or members.termination_date is NULL) then members.id end)                                        as active_employee_count,
        count(distinct case when not members.is_active and members.termination_date <= current_date then members.id end)                                                                         as terminated_employee_count

    from users

    left join user_infos
        on users.id = user_infos.user_id

    left join members
        on users.id = members.user_id

    left join ebenefits_users
        on users.uuid = ebenefits_users.eh_user_uuid

    group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17
)

select * from combined