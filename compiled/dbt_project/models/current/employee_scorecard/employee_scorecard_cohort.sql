

with 
  member_details as (
    select 
      e.id as member_id
      , e.uuid as member_uuid
      , e.user_id
      , e.user_uuid
      -- , e.first_name + ' ' + e.last_name as full_name 
      , e.created_at
      , e.start_date
      , e.termination_date
      , e.active
      , e.organisation_id
      , date_part('year', getdate()) - date_part('year', e.date_of_birth) as age
      , e.gender
      , e.latest_employment_type
      , e.work_country
      , coalesce(o.industry, 'Unknown') as organisation_industry
      , o.country as organisation_country
    from 
      "dev"."employment_hero"."employees" e
      join "dev"."employment_hero"."organisations" o on 
        e.organisation_id = o.id
    where 
      (e.start_date is null or e.start_date<=getdate())
      and (e.termination_date>=e.created_at or e.termination_date is null)
      and o.pricing_tier not ilike '%free%'
  )

select * from member_details