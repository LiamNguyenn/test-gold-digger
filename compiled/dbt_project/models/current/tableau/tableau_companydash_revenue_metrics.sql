--billed revenue
with
fx as (
    select distinct

        case
            when r.currency_code = 'GBP'
                then 'UK'
            when r.currency_code = 'MYR'
                then 'MY'
            when r.currency_code = 'NZD'
                then 'NZ'
            when r.currency_code = 'SGD'
                then 'SG'
            when r.currency_code = 'AUD'
                then 'AU'
        end

        as country,
        cast(r.data_timestamps as date) as date,
        (
            case
                when r.rate = 0
                    then 0
                else aud.rate / r.rate
            end
        )
        as fx_rate
    from
        "dev"."exchange_rates"."rate" as r
    left join (
        select distinct
            cast(data_timestamps as date) as date,
            rate
        from
            exchange_rates.rate
        where
            currency_code = 'AUD'
            and _fivetran_deleted = false
    )
    as aud
        on
            aud.date = cast(r.data_timestamps as date)
    where
        currency_code in (
            'GBP',
            'MYR',
            'NZD',
            'SGD',
            'AUD'
        )
        and _fivetran_deleted = false
),

billed_revenue as (
    select
        invoice_date,
        country,
        sum(
            case
                when
                    product_name like '%HR%'
                    and charge_amount > 0
                    and lower(charge_name) not like '%sms%'
                    and lower(charge_name) not like '%super%'
                    and lower(charge_name) not like '%hour%'
                    then quantity
                else 0
            end
        )
        as hr_invoiced_emps,
        sum(
            case
                when
                    lower(product_name) like '%payroll%'
                    and kp_legacy = false
                    and charge_amount > 0
                    and lower(charge_name) not like '%sms%'
                    and lower(charge_name) not like '%super%'
                    and lower(charge_name) not like '%hour%'
                    then quantity
                else 0
            end
        )
        as payroll_invoiced_emps,
        sum(
            case
                when
                    lower(product_name) like '%payroll%'
                    and kp_legacy = true
                    and charge_amount > 0
                    and lower(charge_name) not like '%sms%'
                    and lower(charge_name) not like '%super%'
                    and lower(charge_name) not like '%hour%'
                    then quantity
                else 0
            end
        )
        as payroll_invoiced_emps_kp_direct,
        sum(
            case
                when product_name like '%HR%'
                    then charge_amount
                else 0
            end
        )
        as hr_billed_revenue,
        sum(
            case
                when
                    lower(product_name) like '%payroll%'
                    and kp_legacy = false
                    then charge_amount
                else 0
            end
        )
        as payroll_billed_revenue,
        sum(
            case
                when
                    lower(product_name) like '%payroll%'
                    and kp_legacy = true
                    then charge_amount
                else 0
            end
        )
        as payroll_billed_revenue_kp_direct,
        sum(
            case
                when
                    lower(product_name) not like '%payroll%'
                    and (
                        product_name
                    )
                    not like '%HR%'
                    then charge_amount
                else 0
            end
        )
        as other_revenue,
        sum(
            case
                when product_name like '%HR%'
                    then charge_amount_aud
                else 0
            end
        )
        as hr_billed_revenue_aud,
        sum(
            case
                when
                    lower(product_name) like '%payroll%'
                    and kp_legacy = false
                    then charge_amount_aud
                else 0
            end
        )
        as payroll_billed_revenue_aud,
        sum(
            case
                when
                    lower(product_name) like '%payroll%'
                    and kp_legacy = true
                    then charge_amount_aud
                else 0
            end
        )
        as payroll_billed_revenue_kp_direct_aud,
        sum(
            case
                when
                    lower(product_name) not like '%payroll%'
                    and (
                        product_name
                    )
                    not like '%HR%'
                    then charge_amount_aud
                else 0
            end
        )
        as other_revenue_aud
    from
        (
            select distinct
                invoice_item.id,
                invoice.invoice_date,
                invoice_item.quantity,
                invoice_item.charge_name,
                (
                    coalesce(a.legacy_account_id_c like 'B%', false)
                )
                as kp_legacy,
                (
                    case
                        when fx.fx_rate is not null
                            then invoice_item.charge_amount * fx.fx_rate
                        -- since fx table only tracks from Jul 23 onwards, we will keep past records with a static rate
                        when lower(a.geo_code_c) = 'au'
                            then invoice_item.charge_amount * 1
                        when lower(a.geo_code_c) = 'nz'
                            then invoice_item.charge_amount * 0.92
                        when
                            lower(a.geo_code_c) = 'uk'
                            or lower(a.geo_code_c) = 'gb'
                            then invoice_item.charge_amount * 1.92
                        when lower(a.geo_code_c) = 'sg'
                            then invoice_item.charge_amount * 1.14
                        when lower(a.geo_code_c) = 'my'
                            then invoice_item.charge_amount * 0.33
                        -- might have orgs without account countries (just in case)
                        when lower(o.country) = 'au'
                            then invoice_item.charge_amount * 1
                        when lower(o.country) = 'nz'
                            then invoice_item.charge_amount * 0.92
                        when
                            lower(o.country) = 'uk'
                            or lower(o.country) = 'gb'
                            then invoice_item.charge_amount * 1.92
                        when lower(o.country) = 'sg'
                            then invoice_item.charge_amount * 1.14
                        when lower(o.country) = 'my'
                            then invoice_item.charge_amount * 0.33
                    end
                )
                as charge_amount_aud,
                invoice_item.charge_amount,
                p.name as product_name,
                case
                    --check acc country first
                    when lower(a.geo_code_c) = 'au'
                        then 'Australia'
                    when lower(a.geo_code_c) = 'uk'
                        then 'United Kingdom'
                    when lower(a.geo_code_c) = 'nz'
                        then 'New Zealand'
                    when lower(a.geo_code_c) = 'my'
                        then 'Malaysia'
                    when lower(a.geo_code_c) = 'sg'
                        then 'Singapore'
                    -- then org level country just in case
                     when lower(o.country) = 'au'
                        then 'Australia'
                    when
                        lower(o.country) = 'gb'
                        or lower(o.country) = 'uk'
                        then 'United Kingdom'
                    when lower(o.country) = 'nz'
                        then 'New Zealand'
                    when lower(o.country) = 'my'
                        then 'Malaysia'
                    when lower(o.country) = 'sg'
                        then 'Singapore'
                    else 'untracked'
                end    as country
            from
                "dev"."zuora"."account" as a
            inner join "dev"."zuora"."invoice"
                on
                    a.id = invoice.account_id
            inner join "dev"."zuora"."invoice_item"
                on
                    invoice.id = invoice_item.invoice_id
            inner join "dev"."zuora"."subscription"
                on
                    invoice_item.subscription_id = subscription.id
            inner join "dev"."zuora"."rate_plan_charge" as rpc
                on
                    rpc.id = invoice_item.rate_plan_charge_id
            inner join "dev"."zuora"."product_rate_plan" as prp
                on
                    rpc.product_rate_plan_id = prp.id
            inner join "dev"."zuora"."product" as p
                on
                    prp.product_id = p.id
            left join "dev"."employment_hero"."organisations" as o
                on
                    o.zuora_account_id = invoice.account_id
            left join fx
                on
                    fx.date = cast(invoice_date as date)
                    and o.country = fx.country
            where
                not a._fivetran_deleted
                and not invoice._fivetran_deleted
                and not invoice_item._fivetran_deleted
                and not p._fivetran_deleted
                and not prp._fivetran_deleted
                and not rpc._fivetran_deleted
                and invoice.status = 'Posted'
                and invoice.posted_date <= current_date
        )
    group by
        1,
        2
),

--active employees
employee_creations as (
    select
        cast(e.created_at as date) as created_date,
        case
            when lower(za.geo_code_c) = 'au'
                then 'Australia'
            when lower(za.geo_code_c) = 'uk'
                then 'United Kingdom'
            when lower(za.geo_code_c) = 'nz'
                then 'New Zealand'
            when lower(za.geo_code_c) = 'my'
                then 'Malaysia'
            when lower(za.geo_code_c) = 'sg'
                then 'Singapore'
            when lower(o2.country) = 'au'
                then 'Australia'
            when lower(o2.country) = 'gb'
                then 'United Kingdom'
            when lower(o2.country) = 'nz'
                then 'New Zealand'
            when lower(o2.country) = 'my'
                then 'Malaysia'
            when lower(o2.country) = 'sg'
                then 'Singapore'
            else 'untracked'
        end                        as country,
        case
            when
                e.eh_member_id is not null
                and e.kp_employee_id is not null
                then 'overlap'
            when e.eh_member_id is not null
                then 'eh'
            when e.kp_employee_id is not null
                then 'keypay'
        end                        as source,
        count(e.*)                 as new_employees,
        count(
            case
                when e.is_paying_eh
                    then e.is_paying_eh
            end
        )
        as new_billed_employees
    from
        "dev"."one_platform"."employees" as e
    left join "dev"."employment_hero"."organisations" as o1
        on
            e.eh_organisation_id = o1.id
            and e.eh_member_id is not null
    left join "dev"."one_platform"."organisations" as o2
        on
            e.kp_business_id = o2.kp_business_id
            and e.eh_member_id is null
            and e.kp_employee_id is not null
    left join "dev"."zuora"."account" as za
        on
            o1.zuora_account_id = za.id
    where
        (
            (e.eh_sub_name not ilike '%demo%' and e.eh_sub_name not ilike '%churn%')
            or e.eh_sub_name is null
        )
        and (
            e.created_at <= e.termination_date
            or e.termination_date is null
        )
    group by
        1,
        2,
        3
    order by
        created_date desc
),

employee_terminations as (
    select
        cast(e.termination_date as date) as termination_date,
        case
            when
                za.geo_code_c is not null
                and lower(za.geo_code_c) = 'au'
                then 'Australia'
            when
                za.geo_code_c is not null
                and lower(za.geo_code_c) = 'uk'
                then 'United Kingdom'
            when
                za.geo_code_c is not null
                and lower(za.geo_code_c) = 'nz'
                then 'New Zealand'
            when
                za.geo_code_c is not null
                and lower(za.geo_code_c) = 'my'
                then 'Malaysia'
            when
                za.geo_code_c is not null
                and lower(za.geo_code_c) = 'sg'
                then 'Singapore'
            when
                o2.country is not null
                and lower(o2.country) = 'au'
                then 'Australia'
            when
                o2.country is not null
                and lower(o2.country) = 'gb'
                then 'United Kingdom'
            when
                o2.country is not null
                and lower(o2.country) = 'nz'
                then 'New Zealand'
            when
                o2.country is not null
                and lower(o2.country) = 'my'
                then 'Malaysia'
            when
                o2.country is not null
                and lower(o2.country) = 'sg'
                then 'Singapore'
            else 'untracked'
        end                              as country,
        case
            when
                e.eh_member_id is not null
                and e.kp_employee_id is not null
                then 'overlap'
            when e.eh_member_id is not null
                then 'eh'
            when e.kp_employee_id is not null
                then 'keypay'
        end                              as source,
        count(e.*)                       as terminations,
        count(
            case
                when e.is_paying_eh
                    then e.is_paying_eh
            end
        )
        as billed_terminations
    from
        "dev"."one_platform"."employees" as e
    left join "dev"."employment_hero"."organisations" as o1
        on
            e.eh_organisation_id = o1.id
            and e.eh_member_id is not null
    left join "dev"."one_platform"."organisations" as o2
        on
            e.kp_business_id = o2.kp_business_id
            and e.eh_member_id is null
            and e.kp_employee_id is not null
    left join "dev"."zuora"."account" as za
        on
            o1.zuora_account_id = za.id
    where
        (
            (e.eh_sub_name not ilike '%demo%' and e.eh_sub_name not ilike '%churn%')
            or e.eh_sub_name is null
        )
        and (
            e.created_at <= e.termination_date
            or e.termination_date is null
        )
        and (
            e.termination_date is not null
            and e.termination_date <= getdate()
        )
    group by
        1,
        2,
        3
),

accumulation as (
    select
        cast(d.date as date)                                                                                                                     as date,
        s.source,
        country.country,
        sum(new_employees) over (partition by s.source, country.country order by d.date rows between unbounded preceding and current row)        as total_employees,
        sum(terminations) over (partition by s.source, country.country order by d.date rows between unbounded preceding and current row)         as terminated_employees,
        sum(new_billed_employees) over (partition by s.source, country.country order by d.date rows between unbounded preceding and current row) as billed_total_employees,
        sum(billed_terminations) over (partition by s.source, country.country order by d.date rows between unbounded preceding and current row)  as billed_terminated_employees,
        coalesce(total_employees, 0) - coalesce(terminated_employees, 0)                                                                         as active_employees,
        coalesce(billed_total_employees, 0) - coalesce(billed_terminated_employees, 0)                                                           as billed_active_employees
    from
        (
            select 'eh' as source
            union distinct
            select 'keypay' as source
            union distinct
            select 'overlap' as source
        )
        as s
    cross join (
        select 'Australia' as country
        union distinct
        select 'United Kingdom' as country
        union distinct
        select 'New Zealand' as country
        union distinct
        select 'Malaysia' as country
        union distinct
        select 'Singapore' as country
        union distinct
        select 'untracked' as country
    )
    as country
    cross join (
        select dateadd('day', 1 - generated_number::int, current_date) as date
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
     + 
    
    p5.generated_number * power(2, 5)
     + 
    
    p6.generated_number * power(2, 6)
     + 
    
    p7.generated_number * power(2, 7)
     + 
    
    p8.generated_number * power(2, 8)
     + 
    
    p9.generated_number * power(2, 9)
     + 
    
    p10.generated_number * power(2, 10)
     + 
    
    p11.generated_number * power(2, 11)
     + 
    
    p12.generated_number * power(2, 12)
    
    
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
     cross join 
    
    p as p5
     cross join 
    
    p as p6
     cross join 
    
    p as p7
     cross join 
    
    p as p8
     cross join 
    
    p as p9
     cross join 
    
    p as p10
     cross join 
    
    p as p11
     cross join 
    
    p as p12
    
    

    )

    select *
    from unioned
    where generated_number <= 7300
    order by generated_number

) -- 365 * 20
    )
    as d
    full outer join employee_creations as c
        on
            s.source = c.source
            and d.date = c.created_date
            and country.country = c.country
    full outer join employee_terminations as t on
        d.date = t.termination_date
        and s.source = t.source
        and country.country = t.country
),

-- billed orgs
accumulation_org as (
    select distinct
        cast(d.month as date)                                                                                                                                                                                                                                                                                     as date,
        case
            when
                eh_organisation_id is not null
                and kp_business_id is not null
                then 'overlap'
            when eh_organisation_id is not null
                then 'eh'
            when kp_business_id is not null
                then 'keypay'
        end                                                                                                                                                                                                                                                                                                       as source,
        (
            case
                when
                    country is not null
                    and lower(country) = 'au'
                    then 'Australia'
                when
                    country is not null
                    and lower(country) = 'gb'
                    then 'United Kingdom'
                when
                    country is not null
                    and lower(country) = 'nz'
                    then 'New Zealand'
                when
                    country is not null
                    and lower(country) = 'my'
                    then 'Malaysia'
                when
                    country is not null
                    and lower(country) = 'sg'
                    then 'Singapore'
                else 'untracked'
            end
        )
        as country,
        count(case when cast(date_trunc('month', created_at) as date) <= cast(d.month as date) and (cast(date_trunc('month', eh_churn_date) as date) >= cast(d.month as date) or eh_churn_date is null) then coalesce(cast(eh_organisation_id as varchar), cast(kp_business_id as varchar)) end)                  as active_orgs,
        count(case when cast(date_trunc('month', created_at) as date) <= cast(d.month as date) and (cast(date_trunc('month', eh_churn_date) as date) >= cast(d.month as date) or eh_churn_date is null) and is_paying_eh then coalesce(cast(eh_organisation_id as varchar), cast(kp_business_id as varchar)) end)
        as active_billed_orgs
    from
        "dev"."one_platform"."organisations"
    cross join (
        select dateadd('month', 1 - generated_number::int, date_trunc('month', getdate())) as month
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
    where generated_number <= 30
    order by generated_number

)
    )
    as d
    where
        (
            (eh_sub_name not ilike '%demo%')
            or eh_sub_name is null
        )
    group by
        1,
        2,
        3
)

select distinct
    a.date,
    a.country,
    b.hr_billed_revenue,
    b.payroll_billed_revenue,
    b.payroll_billed_revenue_kp_direct,
    b.other_revenue,
    b.hr_billed_revenue_aud,
    b.payroll_billed_revenue_aud,
    b.payroll_billed_revenue_kp_direct_aud,
    b.other_revenue_aud,
    b.hr_invoiced_emps,
    b.payroll_invoiced_emps,
    b.payroll_invoiced_emps_kp_direct,
    sum(
        case
            when
                a.source = 'keypay'
                or a.source = 'overlap'
                then a.active_employees
            else 0
        end
    )
    as payroll_employees,
    sum(
        case
            when
                a.source = 'eh'
                or a.source = 'overlap'
                then a.active_employees
            else 0
        end
    )
    as hr_employees,
    sum(
        case
            when
                a.source = 'keypay'
                or a.source = 'overlap'
                then a.billed_active_employees
            else 0
        end
    )
    as billed_payroll_employees,
    sum(
        case
            when
                a.source = 'eh'
                or a.source = 'overlap'
                then a.billed_active_employees
            else 0
        end
    )
    as billed_hr_employees,
    sum(
        case
            when
                o.source = 'keypay'
                or o.source = 'overlap'
                then o.active_orgs
            else 0
        end
    )
    as payroll_organisations,
    sum(
        case
            when
                o.source = 'eh'
                or o.source = 'overlap'
                then o.active_orgs
            else 0
        end
    )
    as hr_organisations,
    sum(
        case
            when
                o.source = 'keypay'
                or o.source = 'overlap'
                then o.active_billed_orgs
            else 0
        end
    )
    as billed_payroll_organisations,
    sum(
        case
            when
                o.source = 'eh'
                or o.source = 'overlap'
                then o.active_billed_orgs
            else 0
        end
    )
    as billed_hr_organisations,
    sum(o.active_billed_orgs) as billed_total_organisations,
    sum(o.active_orgs)        as total_organisations
from
    accumulation as a
left join billed_revenue as b
    on
        a.date = b.invoice_date
        and a.country = b.country
left join accumulation_org as o
    on
        a.date = o.date
        and a.country = o.country
        and a.source = o.source
where
    total_employees is not null
    and a.date >= cast('2020-06-01' as date)
group by
    1,
    2,
    3,
    4,
    5,
    6,
    7,
    8,
    9,
    10,
    11,
    12,
    13
order by
    a.date desc