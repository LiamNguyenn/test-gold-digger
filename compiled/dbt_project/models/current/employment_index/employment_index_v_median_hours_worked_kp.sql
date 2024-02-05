

with
    kp_business_industry as (
        select
            m.id,
            case
                when m.industry = 'Other'
                then 'Other'
                when m.industry != 'Other' and m.industry is not null
                then i.consolidated_industry
                else null
            end as industry
        from
            (
                select
                    b.id,
                    case
                        when
                            b.industry_id is null
                            and b.industry_name is not null
                        then 'Other'
                        when
                            b.industry_id is null
                            and b.industry_name is null
                            and z.primary_industry is not null
                            and z.primary_industry != ''
                        then z.primary_industry
                        when b.industry_id is not null
                        then i.name
                        else null
                    end as industry
                from "dev"."keypay_dwh"."business" as b
                left join
                    (select id, name from "dev"."keypay"."industry") as i on b.industry_id = i.id
                left join
                    (select _id, primary_industry from keypay.zoom_info) as z
                    on b.id = z._id
            ) as m
        left join
            "dev"."one_platform"."industry" as i
            on regexp_replace(m.industry, '\\s', '')
            = regexp_replace(i.keypay_industry, '\\s', '')
            or regexp_replace(m.industry, '\\s', '')
            = regexp_replace(i.zoom_info_primary_industry, '\\s', '')
            or regexp_replace(m.industry, '\\s', '')
            = regexp_replace(i.eh_industry, '\\s', '')
    ),

    keypay_employees as (
        select
            e.id,
            e.date_created,
            e.date_of_birth,
            e.residential_suburb_id,
            e.end_date,
            e.gender,
            e.business_id,
            e.start_date,
            et.description as employment_type_description
        from "dev"."keypay_dwh"."employee" e
        join "dev"."keypay_dwh"."business" as b on b.id = e.business_id
        left join "dev"."keypay"."white_label" as wl on b.white_label_id = wl.id
        left join
            "dev"."keypay"."tax_file_declaration" as tfd
            on tfd.id = e.tax_file_declaration_id
            and e.id = tfd.employee_id  -- AU
        left join "dev"."keypay"."employment_type" as et on et.id = tfd.employment_type_id
        where
            (
                e.end_date is null
                or e.end_date >= e.date_created
            )
            and surname not ilike ('%zzz%')
            and firstname not ilike ('%zzz%')
            and not (
                surname
                ~* '(^|[ !@#$%^&*(),.?":{}|<>]|\d+)(test|demo)($|[ !@#$%^&*(),.?":{}|<>]|\d+)'
                or firstname
                ~* '(^|[ !@#$%^&*(),.?":{}|<>]|\d+)(test|demo)($|[ !@#$%^&*(),.?":{}|<>]|\d+)'
            )
    ),

    employee_details as (
        select
            m.id,
            m.gender,
            m.date_of_birth,
            m.employment_type_description,
            m.start_date,
            m.end_date,
            s.postcode as residential_postcode,
            s.name as residential_suburb,
            -- , l.state as residential_state			
            case
                when s.state ~* '(South Australia|SA)'
                then 'SA'
                when s.state ~* '(Northern Territory|NT)'
                then 'NT'
                when s.state ~* '(Victoria|VIC)'
                then 'VIC'
                when s.state ~* '(New South|NSW)'
                then 'NSW'
                when s.state ~* '(Queensland|QLD)'
                then 'QLD'
                when s.state ~* '(Tasmania|TAS)'
                then 'TAS'
                when s.state ~* '(Western Australia|WA)'
                then 'WA'
                when s.state ~* '(Australian Capital Territory|ACT)'
                then 'ACT'
                else null
            end as residential_state,
            s.country as residential_country
        -- distinct lower(surname) as sname
        -- ,lower(first_name) as fname
        -- count(*)
        from keypay_employees m
        left join "dev"."keypay_dwh"."suburb" s on m.residential_suburb_id = s.id
    -- multiple matches of state for one postcode
    -- left join (select distinct postcode, state from
    -- csv.australian_postcodes_localities where sa_4_code_2016 is not null)l on
    -- s.postcode = l.postcode
    ),

    dates as (
        select
            dateadd(
                'month', -generated_number::int, (date_trunc('month', add_months(current_date, 1)))
            )::date as month
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
    
    

    )

    select *
    from unioned
    where generated_number <= 300
    order by generated_number

)
        where month >= '2017-01-01'
    -- where month >= '2023-02-01'
    ),

    keypay_pay_run_summary as (
        select
            -- d.month,
            coalesce(prt.employee_id, prth.employee_id) as employee_id,
            pr.business_id,
            pr.invoice_id,
            -- , DATEADD(month, 1, DATEFROMPARTS(year(pr.DateFirstFinalised),
            -- month(pr.DateFirstFinalised), 1)) as BillingMonth
            dateadd('DAY', 1, last_day(pr.date_first_finalised::date))::date
            as billing_month,
            coalesce(
                prt.is_excluded_from_billing, prth.is_excluded_from_billing
            ) as is_excluded_from_billing,
            pay_period_starting,
            pay_period_ending,
            sum(coalesce(prt.total_hours, prth.total_hours)) as total_hours_combined
        from  "dev"."keypay"."payrun" pr
        join "dev"."keypay_dwh"."business" b on pr.business_id = b.id
        left join
            "dev"."keypay"."payrun_total" prt
            on prt.payrun_id = pr.id
            and split_part(pr._file, 'Shard', 2) = split_part(prt._file, 'Shard', 2)
            and pr.date_first_finalised::date >= '2022-01-01'
            and prt.is_excluded_from_billing = 0
        left join
            "dev"."keypay"."payrun_total_history" prth
            on prth.payrun_id = pr.id
            and split_part(pr._file, 'Shard', 2) = split_part(prth._file, 'Shard', 2)
            and pr.date_first_finalised::date < '2022-01-01'
            and prth.is_excluded_from_billing = 0
        where
            pr.date_first_finalised is not null --and pr.date_first_finalised != 'NULL'
            -- and pr.DateFirstFinalised >= ''''',@fromDate,'''''      and
            -- pr.DateFirstFinalised <= ''''',@toDate,'''''
            and (
                b.to_be_deleted is null
                or b.to_be_deleted = 0
--                 or b.to_be_deleted = ''
--                 or b.to_be_deleted = 'NULL'
--                 or b.to_be_deleted = 'False'
            )  -- ISNULL(b.to_be_deleted, 0) = 0
            -- and prt.employee_id = '320280'
            -- and invoice_id = '3752585'
        group by 1, 2, 3, 4, 5, 6, 7
    ),

    monthly_hours as (
        select
            d.month,
            employee_id,
            business_id,
            invoice_id,
            -- pay_period_starting,
            -- pay_period_ending,
            sum(
                total_hours_combined * (
                    datediff(
                        day,
                        case
                            when d.month <= pay_period_starting::date
                            then pay_period_starting::date
                            else d.month
                        end,
                        case
                            when dateadd('month', 1, d.month) > pay_period_ending::date
                            then pay_period_ending::date
                            else dateadd('month', 1, d.month)
                        end
                    )
                    + 1
                )
                / (
                    datediff(day, pay_period_starting::date, pay_period_ending::date)
                    + 1
                )
            ) as monthly_hours

        from dates as d
        join
            keypay_pay_run_summary
            on (
                pay_period_starting::date < dateadd('month', 1, d.month)
                and d.month <= pay_period_ending::date
            )
        group by 1, 2, 3, 4
    ),

    total_employees_per_business as (
        select month, business_id, count(distinct employee_id) as total_employees
        from monthly_hours
        group by 1, 2
    ),

    billed_business as (
        select distinct  -- DATE_TRUNC('month', i.date::date)::date as month
            case
                when ili.business_id is null
                then b.id
                else ili.business_id
            end as business_id
        from "dev"."keypay"."invoice_line_item" ili
        join
            "dev"."keypay"."invoice" i
            on i.id = ili.invoice_id
            and split_part(i._file, 'Shard', 2) = split_part(ili._file, 'Shard', 2)
        left join
            "dev"."keypay_dwh"."business" b
            on ili.abn = b.abn
            and ili.business_id is null
    )

select
    mh.employee_id,
    e.residential_state,
    mh.business_id,
    i.industry,
    total_emps.total_employees,
    mh.invoice_id,
    mh.month,
    mh.monthly_hours,
    -- , pr.payg_withholding_amount
    -- , pr.help_amount
    -- , pr.super_contribution      
    case when e.gender = 'F' then 'Female' when e.gender = 'M' then 'Male' end as gender
    ,
    datediff('year', e.date_of_birth, mh.month) as age,
    case
        when
            employment_type_description = ''
            or employment_type_description is null
            or employment_type_description = 'NULL'
        then null
        when employment_type_description = 'Full Time'
        then 'Full-time'
        when employment_type_description = 'Part Time'
        then 'Part-time'
        else employment_type_description
    end as employment_type,
    e.start_date,
    e.end_date
-- , (pr.monthly_gross_earnings-avg(pr.monthly_gross_earnings) over ()) /
-- (stddev(pr.monthly_gross_earnings) over ()) as z_score_earnings
-- , (pr.total_hours-avg(pr.total_hours) over ()) / (stddev(pr.total_hours) over ())
-- as z_score_hours
-- , (hourly_rate-avg(hourly_rate) over ()) / (stddev(hourly_rate) over ()) as
-- z_score_hourly_rate
from monthly_hours mh
join employee_details e on mh.employee_id = e.id
join "dev"."keypay_dwh"."business" b on mh.business_id = b.id
left join "dev"."keypay"."white_label" as wl on b.white_label_id = wl.id
join
    -- keypay.business_monthly_summary_062022 as bs 
    billed_business bs on bs.business_id = b.id and bs.business_id is not null  -- and bs.month = pr.billing_month      
left join
    total_employees_per_business total_emps
    on total_emps.month = mh.month
    and total_emps.business_id = b.id
left join kp_business_industry as i on b.id = i.id
where
    mh.monthly_hours < 24 * 31
    -- and not mh.is_excluded_from_billing
    and (
        employment_type_description != 'Superannuation Income Stream'
        or employment_type_description is null
    )
    and (
        employment_type_description != 'Labour Hire'
        or employment_type_description is null
    )
    and mh.invoice_id is not null
    and (wl.region_id is null or wl.region_id = 1)  -- AU business      
    and (
        reseller_id is null
        or reseller_id not in (511, 829, 22, 708, 755, 790, 669)
    )  -- exclude Test Partners
    and abn != 11111111111
    and datediff('day', getdate(), b.commence_billing_from::date) < 180
    -- and b.name ilike '%test%' or name ilike '%demo%'     
    and mh.month is not null