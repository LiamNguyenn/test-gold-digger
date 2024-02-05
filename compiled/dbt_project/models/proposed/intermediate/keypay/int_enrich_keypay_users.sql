



with users as (
    select
        id,
        first_name,
        last_name,
        email,
        is_active,
        is_admin

    from "dev"."stg__keypay"."user"

    where date_trunc('day', _transaction_date) = '2024-01-29'
),

user_transformed as (
    select
        id as keypay_user_id,
        first_name,
        last_name,
        email,
        is_active,
        is_admin

    from users
),

employees as (
    select employee.*

    from "dev"."stg__keypay_dwh"."employee" as employee

    qualify row_number() over (partition by id order by _transaction_date desc) = 1
),

eh_employees as (
    select *

    from "dev"."staging"."stg_postgres_public__members"

    where organisation_id = 8701
),

user_employees as (
    select *

    from "dev"."stg__keypay"."user_employee"

    where date_trunc('day', _transaction_date) = '2024-02-02'
),

ebenefits_users as (
    select ebenefits.keypay_user_id

    from "dev"."staging"."stg_ebenefits__user_created" as ebenefits

    group by 1
),

joined as (
    select
        users.*,
        ebenefits_users.keypay_user_id is not NULL                                                                            as has_swag_profile,
        count(distinct eh_employees.id) > 0                                                                                   as is_current_eh_employee,
        count(distinct case when employees.end_date is NULL or employees.end_date > current_date then employees.id end) > 0   as is_active_employee,
        count(distinct case when employees.end_date is NULL or employees.end_date > current_date then employees.id end)       as active_employee_count,
        count(distinct case when employees.end_date is not NULL and employees.end_date <= current_date then employees.id end) as terminated_employee_count

    from user_transformed as users

    left join user_employees
        on users.keypay_user_id = user_employees.user_id

    left join employees
        on user_employees.employee_id = employees.id

    left join eh_employees
        on employees.id = eh_employees.external_payroll_employee_id

    left join ebenefits_users
        on users.keypay_user_id = ebenefits_users.keypay_user_id

    group by 1,2,3,4,5,6,7
)

select * from joined