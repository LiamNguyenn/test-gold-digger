

WITH
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
    org_size as (
        select
            d.date,
            organisation_id,
            count(*) as total_employees,
            case
                when total_employees < 20
                then '1-19'
                when total_employees between 20 and 199
                then '20-199'
                when total_employees > 200
                then '200+'
            end as company_size
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
            o.pricing_type != 'demo'
        group by 1, 2
    )
SELECT * FROM org_size