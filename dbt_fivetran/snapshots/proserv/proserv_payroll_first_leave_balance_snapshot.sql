{% snapshot proserv_payroll_first_leave_balance_snapshot %}

{{
    config(
    alias='payroll_first_leave_balance_snapshot',
    target_schema="proserv",
    strategy='check',
    unique_key='business_id',
    check_cols=['open_balance_imported'],
    invalidate_hard_deletes=True,
    )
}}

select distinct e.business_id
, true as open_balance_imported
    from {{ref('keypay_leave_accrual')}} l
    join {{ref('keypay_accrual_status')}} s on l.accrual_status_id = s.id
    join {{ref('keypay_dwh_employee')}} e on l.employee_id = e.id
    where l.accrued_amount != 0
    and s.description in ('Leave Adjustment', 'Leave Termination', 'Manually Applied', 'Manually Overridden', 'Reconciliation Adjustment')
    group by 1

{% endsnapshot %}