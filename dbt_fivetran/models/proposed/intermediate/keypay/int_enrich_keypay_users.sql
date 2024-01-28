{% set latest_user_transaction_date = get_latest_transaction_date_v2(ref("stg__keypay__user")) %}
{% set latest_employee_transaction_date = get_latest_transaction_date_v2(ref("stg__keypay_dwh__employee")) %}
{% set latest_user_employee_transaction_date = get_latest_transaction_date_v2(ref("stg__keypay__user_employee")) %}

with users as (
    select
        id,
        first_name,
        last_name,
        email,
        is_active,
        is_admin

    from {{ ref("stg__keypay__user") }}

    where date_trunc('day', _transaction_date) = '{{ latest_user_transaction_date }}'
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

    from {{ ref("stg__keypay_dwh__employee") }} as employee

    qualify row_number() over (partition by id order by _transaction_date desc) = 1
),

eh_employees as (
    select *

    from {{ ref("stg_postgres_public__members") }}

    where organisation_id = 8701
),

user_employees as (
    select *

    from {{ ref("stg__keypay__user_employee") }}

    where date_trunc('day', _transaction_date) = '{{ latest_user_employee_transaction_date }}'
),

joined as (
    select
        users.*,
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

    group by 1, 2, 3, 4, 5, 6
)

select * from joined
