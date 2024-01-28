{{
    config(
        materialized='incremental',
        alias='companydash_mmau_exclu_payroll'
    )
}}

with dates as (
select
          DATEADD('day', -generated_number::int, (current_date + 1)) date
        from ({{ dbt_utils.generate_series(upper_bound=3) }})
          where "date" < (select date_trunc('day', max("timestamp")) from {{ ref('customers_events') }})
{% if is_incremental() %}           
        and date > (select max(date) from {{this}})
{% endif %}          
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
      inner join {{ ref('customers_events')}} as e on
      e.timestamp < dateadd(day, 1, dates.date)
      and e.timestamp >= dateadd(day, -29, dates.date)
      left join {{ ref('employment_hero_employees') }} ee on ee.uuid = e.member_uuid 
      left join {{ ref('employment_hero_organisations') }} o on
        ee.organisation_id = o.id
      left join {{ source('eh_product', 'module_ownership') }} mo on
        mo.event_module = e.module
    where
      e.timestamp < cast( current_date as date)
      and e.timestamp >= cast( dateadd('day',-29,current_date) as date)
    group by
      1
      , 2
