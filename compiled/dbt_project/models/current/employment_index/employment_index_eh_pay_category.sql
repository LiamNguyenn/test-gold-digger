

with
    eh_pay_category_mapping as (
        select pay_category_name, category from "dev"."pay_category_mappings"."eh_pay_category_mapping"
    ),
    dates as (
        select
            dateadd(
                'month', -generated_number::int, (date_trunc('month', add_months(current_date, 1)))
            )::date date
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
        where date >= '2017-01-01'
    ),

    net_earnings_table as (
        select *
        from "dev"."postgres_public"."earnings_lines" el
        left join
            eh_pay_category_mapping epcm
            on el.pay_category_name = epcm.pay_category_name
        left join
            "dev"."postgres_public"."payslips" ps on el.payslip_id = ps.id
        where
            --country is not null
            currency = 'AUD' or currency is null
            and not ps._fivetran_deleted
            --and el.pay_category_name = 'Salary'
    ),

    monthly_net_earnings as (
        select
            d.date as month,
            member_id,
            category,
            sum(
                net_pay * (
                    datediff(
                        day,
                        case
                            when d.date <= net.pay_period_starting
                            then net.pay_period_starting
                            else d.date
                        end,
                        case
                            when dateadd('month', 1, d.date) > net.pay_period_ending
                            then net.pay_period_ending
                            else dateadd('month', 1, d.date)
                        end
                    )
                    + 1
                )
                / (datediff(day, net.pay_period_starting, net.pay_period_ending) + 1)
            ) as net_earnings
        from dates as d
        join
            net_earnings_table net
            on net.pay_period_starting < dateadd('month', 1, d.date)
            and d.date <= net.pay_period_ending
        where currency = 'AUD' or currency is null
        group by 1, 2, 3
    ),

    monthly_category_breakdown as (
        select month, category, sum(net_earnings)
        from monthly_net_earnings
        group by 1, 2
    ),

    org_size as (
        select d.date, organisation_id, count(*) as total_employees
        from dates d
        join
            "dev"."employment_hero"."employees" as e
            on e.start_date <= d.date
            and (e.termination_date >= d.date or e.termination_date is null)
            and e.created_at <= d.date
        join
            "dev"."employment_hero"."organisations" as o
            on o.id = e.organisation_id
            and o.created_at <= d.date
        where  -- e.active and
            o.pricing_type != 'demo' and o.country = 'AU'
        group by 1, 2
    ),

    eh_business_industry as (
        select
            m.id,
            case
                when m.industry is not null then i.consolidated_industry else null
            end as industry
        from "dev"."employment_hero"."organisations" as m
        left join
            "dev"."one_platform"."industry" as i
            on regexp_replace(m.industry, '\\s', '')
            = regexp_replace(i.eh_industry, '\\s', '')
    )

select
    d.date as month,
    o.id as organisation_id,
    i.industry,
    os.total_employees,
    oa.state as org_state,
    p.member_id,
    e.work_country,
    p.category,
    case
        when ea.state ~* '(South Australia|SA)'
        then 'SA'
        when ea.state ~* '(Northern Territory|NT)'
        then 'NT'
        when ea.state ~* '(Victoria|VIC)'
        then 'VIC'
        when ea.state ~* '(New South|NSW)'
        then 'NSW'
        when ea.state ~* '(Queensland|QLD)'
        then 'QLD'
        when ea.state ~* '(Tasmania|TAS)'
        then 'TAS'
        when ea.state ~* '(Western Australia|WA)'
        then 'WA'
        when ea.state ~* '(Australian Capital Territory|ACT)'
        then 'ACT'
        else null
    end as residential_state,
    case
        when gender ~* '^f' then 'Female' when gender ~* '^m' then 'Male'
    end as gender,
    datediff('year', date_of_birth, d.date) as age,
    case
        when employment_type like 'Full%' then 'Full-time' else employment_type
    end as employment_type,
    net_earnings,
    z_net_earnings
from dates d
join
    (
        select
            *,
            (net_earnings - avg(net_earnings) over ())
            / (stddev(net_earnings) over ()) as z_net_earnings
        from monthly_net_earnings
    ) as p
    on d.date = p.month
join "dev"."employment_hero"."employees" as e on p.member_id = e.id
join
    "dev"."postgres_public"."employment_histories" as h
    on coalesce(h.start_date, h.created_at) <= d.date
    and (h.end_date >= d.date or h.end_date is null)
    and h.member_id = e.id
    and not h._fivetran_deleted
join
    "dev"."employment_hero"."organisations" as o
    on o.id = e.organisation_id
    and o.created_at <= d.date
join org_size os on os.date = p.month and os.organisation_id = o.id
join eh_business_industry i on o.id = i.id
left join
    "dev"."postgres_public"."addresses" oa
    on o.primary_address_id = oa.id
    and not oa._fivetran_deleted
left join
    "dev"."postgres_public"."addresses" ea
    on e.address_id = ea.id
    and not ea._fivetran_deleted
where  -- e.active and 
    o.pricing_type != 'demo' and o.country = 'AU'