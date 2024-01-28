{{
    config(
        materialized='table',
        alias='guided_payroll_milestones'
    )
}}

with account as (
    select 
    distinct a.id as account_id, 
    a.name as account_name, 
    ipc.name as professional_service_name,
    o.id_c as kp_business_id,
    kb.country as country,
    opp.id as opp_id,
    opp.close_date, 
    nullif(opp.opportunity_employees_c, 0) as opportunity_employees,     
    ipc.service_offering_c,
    ipc.project_started_date_c,
    ipc.expected_go_live_c,
    ipc.project_completion_date_c,
    ipc.closed_status_c
    --, p_2.name as product_name
    from {{source('salesforce', 'account')}} as a
    join {{source('salesforce', 'implementation_project_c')}} as ipc on ipc.account_c = a.id
    left join {{source('salesforce', 'keypay_org_c')}} as o on o.professional_service_project_c is not null and ipc.id = o.professional_service_project_c and not o.is_deleted
    left join {{ref('keypay_business_traits')}} as kb on o.id_c = kb.id    
    left join {{source('salesforce', 'opportunity')}} as opp on ipc.opportunity_c is not null and ipc.opportunity_c = opp.id and not opp.is_deleted and opp.stage_name = 'Won'
    where not a.is_deleted
        and not ipc.is_deleted
        and ipc.service_offering_c ~* '(guided payroll|combined journey)'
        --and (ipc.stage_c != 'Cancelled' or ipc.stage_c is null)
        --and (a.customer_stage_c !~ '(Churned|Lost Before Activated)' or a.customer_stage_c is null)
)

, payroll_settings_completed as (
    select business_id, min(dbt_valid_from)::date as completed_at
    from {{ref('proserv_payroll_settings_snapshot')}}
    where is_payroll_settings_completed
    group by 1
)

, primary_chart_of_accounts as (
    select business_id, min(dbt_valid_from)::date as completed_at
    from {{ref('proserv_payroll_primary_chart_of_accounts_mapped_snapshot')}}
    where are_default_primary_accounts_mapped
    group by 1
)

, open_balance as (
    select business_id, min(dbt_valid_from)::date as completed_at
    from {{ref('proserv_payroll_first_leave_balance_snapshot')}}
    where open_balance_imported
    group by 1
)

, payslips_first_published as (
    select business_id, min(Notification_Date::date) as first_published_at
    from {{ref('keypay_payrun')}}
    where Notification_Date is not null 
    group by 1
)

, employees_created as (
    select business_id
    , count(*) as employees_created
    from {{ref('keypay_dwh_employee')}}
    where (end_date::timestamp is null or end_date::timestamp > date_created::timestamp)
    group by 1
)

, billed_employees as (    
    select business_id
    , billing_month as last_billing_month
    , count(distinct employee_id) as last_billed_employees
    from ( 
        select *
        , rank() over (partition by business_id order by billing_month desc) as rnk
        from {{ref('keypay_t_pay_run_total_monthly_summary')}}
        where not is_excluded_from_billing
        and billing_month = DATE_TRUNC('month', getdate())::date
    )
    where rnk = 1
    group by 1,2
)

, au_stp_registered as (
select business_id, min(pe.date_response_received_utc::date) as stp_registered_at
from {{ref('keypay_pay_event')}} pe
join {{ref('keypay_pay_run_lodgement_data')}}  ld on pe.pay_run_lodgement_data_id = ld.id and SPLIT_PART(pe._file, 'Shard', 2) = SPLIT_PART(ld._file, 'Shard', 2)
where not pe.is_deleted
--and pe.status = 8
and ld.status = 6
and pe.pay_run_id is not null
and pe.date_response_received_utc::date is not null
group by 1
)

, nz_ird_registered as (
    select business_id, min(pdf.date_submitted::date) as ird_registered_at
    from {{ref('keypay_pay_day_filing')}} pdf    
    where pdf.status = 4
    and pdf.pay_run_id is not null    
    group by 1
)

, uk_hmrc_registered as (    
    select pr.business_id, min(pr.date_first_finalised::date) as hmrc_registered_at    
    from {{ref('keypay_pay_run_lodgement_data')}} pld    
    join {{ref('keypay_payrun')}} pr on pr.pay_run_lodgement_data_id = pld.id and SPLIT_PART(pr._file, 'Shard', 2) = SPLIT_PART(pld._file, 'Shard', 2)
    where pld.status = 6
    group by 1
)

select
    distinct  
    {{ dbt_utils.generate_surrogate_key(['a.account_id', 'a.kp_business_id', 'a.professional_service_name']) }} as id
    , a.account_id
    , a.account_name
    , a.service_offering_c
    , a.professional_service_name
    , a.project_started_date_c as project_started_date
    , a.expected_go_live_c as expected_go_live
    , a.project_completion_date_c as project_completion_date
    , a.closed_status_c as closed_status
    , a.opportunity_employees
    , a.kp_business_id
    , ss.completed_at as payroll_settings_completed_at
    , ca.completed_at as primary_chart_of_accounts_mapped_at
    , ob.completed_at as opening_balances_imported_at    
    , ps.first_published_at as payslips_first_published_at
    , sr.stp_registered_at
    , ir.ird_registered_at
    , hr.hmrc_registered_at
    , ec.employees_created
    , be.last_billed_employees
    , be.last_billing_month
from account a
left join employees_created ec on ec.business_id = a.kp_business_id
left join billed_employees be on be.business_id = a.kp_business_id
left join payslips_first_published ps on a.kp_business_id = ps.business_id
left join payroll_settings_completed ss on ss.business_id = a.kp_business_id
left join primary_chart_of_accounts ca on ca.business_id = a.kp_business_id
left join open_balance ob on ob.business_id = a.kp_business_id
left join au_stp_registered sr on sr.business_id = a.kp_business_id
left join nz_ird_registered ir on ir.business_id = a.kp_business_id
left join uk_hmrc_registered hr on hr.business_id = a.kp_business_id