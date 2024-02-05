
select
id, employee_id, accrued_amount, accrual_status_id, "_transaction_date",
                                      "_etl_date", "_modified", "_file"
from
    (
        select
id, employee_id, accrued_amount, accrual_status_id, "_transaction_date",
                                      "_etl_date", "_modified", "_file",
            row_number() over (partition by id, employee_id order by _transaction_date desc) as seqnum
        from "dev"."stg__keypay"."leave_accrual" t
    ) t
where
    seqnum = 1
    

        -- this filter will only be applied on an incremental run
        -- (uses > to include records whose timestamp occurred since the last run of this model)
        and _transaction_date > (select max(_transaction_date) from "dev"."int__keypay"."leave_accrual")

    