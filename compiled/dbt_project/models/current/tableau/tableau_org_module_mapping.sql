select distinct
    date(d.month) as date,
    mo.organisation_id,
    aa.name as zuora_account_name,
    aa.account_id as zuora_account_id,
    aa.geo_code_c zuora_account_geo_code,
    aa.hr_billed_revenue,
    aa.payroll_billed_revenue,
    o.subscription_id,
    o.name org_name,
    o.created_at org_created_at,
    o.subscription_renewal org_subscription_renewal,
    o.calendar_year_type org_calendar_year_type,
    o.superannuation_fund_id org_superannuation_fund_id,
    o.number_of_employees,
    o.super_selection_timestamp,
    o.country as org_country,
    o.estimated_number_of_employees,
    o.currency,
    o.locale,
    o.time_zone,
    o.launched,
    o.industry,
    o.connected_app,
    o.pricing_type,
    o.pricing_tier,
    o.is_paying_eh,
    o.churn_date,
    o.is_demo,
    mo.module,
    mow.product_family,
    mo.monthly_users,
    dbo.monthly_users as total_monthly_users
from
    -- create month ends
    (
        select dateadd('day', -1, dateadd('month', 1 - generated_number::int, date_trunc('month', getdate()))) as month
        from (

    

    with p as (
        select 0 as generated_number union all select 1
    ), unioned as (

    select

    
    p0.generated_number * power(2, 0)
     + 
    
    p1.generated_number * power(2, 1)
     + 
    
    p2.generated_number * power(2, 2)
     + 
    
    p3.generated_number * power(2, 3)
     + 
    
    p4.generated_number * power(2, 4)
    
    
    + 1
    as generated_number

    from

    
    p as p0
     cross join 
    
    p as p1
     cross join 
    
    p as p2
     cross join 
    
    p as p3
     cross join 
    
    p as p4
    
    

    )

    select *
    from unioned
    where generated_number <= 25
    order by generated_number

)
    )
    d
-- join for daumau by orgs / modules
inner join
    "dev"."mp"."daumau_by_module_org" mo
    on date(mo.date) = date(d.month)
    and mo.organisation_id is not null
    and mo.date >= dateadd('month', -24, current_date)
-- join for org info
inner join "dev"."employment_hero"."organisations" o on o.id = mo.organisation_id
-- join for zuora billed revenue and zuora account info
left join
    (
        select distinct
            account_id,
            geo_code_c,
            name,
            date_trunc('month', invoice_date) as month,
            sum(case when product_name like '%HR%' then charge_amount else 0 end) as hr_billed_revenue,
            sum(case when lower(product_name) like '%payroll%' then charge_amount else 0 end) as payroll_billed_revenue
        from
            (
                select distinct
                    a.id account_id,
                    a.geo_code_c,
                    a.name,
                    invoice_item.id,
                    invoice.invoice_date,
                    invoice_item.charge_amount,
                    p.name as product_name
                from "dev"."zuora"."account" a
                inner join "dev"."zuora"."invoice" on a.id = invoice.account_id
                inner join "dev"."zuora"."invoice_item" on invoice.id = invoice_item.invoice_id
                inner join "dev"."zuora"."subscription" on invoice_item.subscription_id = subscription.id
                inner join "dev"."zuora"."rate_plan_charge" rpc on rpc.id = invoice_item.rate_plan_charge_id
                inner join "dev"."zuora"."product_rate_plan" prp on rpc.product_rate_plan_id = prp.id
                inner join "dev"."zuora"."product" p on p.id = prp.product_id
                where
                    not a._fivetran_deleted
                    and not invoice._fivetran_deleted
                    and not invoice_item._fivetran_deleted
                    and not p._fivetran_deleted
                    and not prp._fivetran_deleted
                    and not rpc._fivetran_deleted
                    and invoice.status = 'Posted'
                    and invoice.posted_date <= current_date
                    and invoice.posted_date >= dateadd('month', -26, current_date)
            )
        group by 1, 2, 3, 4
    )
    aa on aa.account_id = o.zuora_account_id and aa.month = date_trunc('month', d.month)
-- join for product family 
left join "dev"."eh_product"."module_ownership" mow on mow.event_module = mo.module
-- join for TOTAL org mau
left join
    (
        select distinct date, organisation_id, monthly_users
        from "dev"."mp"."daumau_by_org"
        where date >= dateadd('month', -24, current_date)
    )
    dbo on mo.organisation_id = dbo.organisation_id and date(d.month) = date(dbo.date)