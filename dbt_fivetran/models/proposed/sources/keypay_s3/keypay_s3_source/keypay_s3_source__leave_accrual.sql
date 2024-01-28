{{ config(
    alias="leave_accrual_source",
    materialized="incremental",
    unique_key = ['id', 'employee_id'],
    incremental_strategy='delete+insert'
) }}
select
    {{ dbt_utils.star(from=source('keypay_s3', 'leave_accrual')) }}
from {{ source('keypay_s3', 'leave_accrual') }}
where 1 = 1
{% if is_incremental() %}

        -- this filter will only be applied on an incremental run
        -- (uses > to include records whose timestamp occurred since the last run of this model)
        and _transaction_date > (select max(_transaction_date) from {{ this }})

    {% endif %}
qualify row_number() over (partition by id, employee_id order by _transaction_date desc) = 1
