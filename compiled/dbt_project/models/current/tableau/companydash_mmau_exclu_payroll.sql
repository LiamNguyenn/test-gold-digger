

with dates as (
select
          DATEADD('day', -generated_number::int, (current_date + 1)) date
        from (

    

    with p as (
        select 0 as generated_number union all select 1
    ), unioned as (

    select

    
    p0.generated_number * power(2, 0)
     + 
    
    p1.generated_number * power(2, 1)
    
    
    + 1
    as generated_number

    from

    
    p as p0
     cross join 
    
    p as p1
    
    

    )

    select *
    from unioned
    where generated_number <= 3
    order by generated_number

)
          where "date" < (select date_trunc('day', max("timestamp")) from "dev"."customers"."events")
           
        and date > (select max(date) from "dev"."tableau"."companydash_mmau_exclu_payroll")
          
)

select
      dates.date
      , ( case
          when lower(ee.work_country) = 'au'
            then 'Australia'
          when lower(ee.work_country) = 'gb'
            then 'United Kingdom'
          when lower(ee.work_country) = 'sg'
            then 'Singapore'
          when lower(ee.work_country) = 'my'
            then 'Malaysia'
          when lower(ee.work_country) = 'nz'
            then 'New Zealand'
          else 'untracked'
        end
      )
      as country
      , count(distinct coalesce(e.user_id, e.user_email)) as HR_MAU
      , count(distinct(case when lower(mo.product_family) in ('talent management', 'talent acquisition') then coalesce(e.user_id, e.user_email) end)
      )
      as Talent_MAU
      , count(distinct(case when lower(mo.product_family) in('ebenefits') then coalesce(e.user_id, e.user_email) end)
      )
      as eben_MAU
      , count(distinct case when o.is_paying_eh = true then coalesce(e.user_id, e.user_email) end) as HR_MMAU
      , count(distinct(case when lower(mo.product_family) in ('talent management', 'talent acquisition') and o.is_paying_eh = true is not null then coalesce(e.user_id, e.user_email) end)
      )
      as Talent_MMAU
      , count(distinct(case when lower(mo.product_family) in('ebenefits') and o.is_paying_eh = true then coalesce(e.user_id, e.user_email) end)
      )
      as eben_MMAU
    from
      dates
      inner join "dev"."customers"."events" as e on
      e.timestamp < dateadd(day, 1, dates.date)
      and e.timestamp >= dateadd(day, -29, dates.date)
      left join "dev"."employment_hero"."employees" ee on ee.uuid = e.member_uuid 
      left join "dev"."employment_hero"."organisations" o on
        ee.organisation_id = o.id
      left join "dev"."eh_product"."module_ownership" mo on
        mo.event_module = e.module
    where
      e.timestamp < cast( current_date as date)
      and e.timestamp >= cast( dateadd('day',-29,current_date) as date)
    group by
      1
      , 2