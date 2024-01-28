with
  dates as (
    select distinct
      DATEADD('day', -generated_number::int, current_date) as "date"
    from ({{ dbt_utils.generate_series(upper_bound=3000) }})
  )
  , project_status_history as (
    select distinct
      ip.id project_id 
      ,(case
          when lower(a.geo_code_c) = 'au'
            then 'Australia'
          when lower(a.geo_code_c) = 'uk'
            then 'United Kingdom'
          when lower(a.geo_code_c) = 'sg'
            then 'Singapore'
          when lower(a.geo_code_c) = 'my'
            then 'Malaysia'
          when lower(a.geo_code_c) = 'nz'
            then 'New Zealand'
          else 'untracked'
        end
      )
      as country
      , ip.service_offering_c service_offering
      , iph.created_date::date as agg_date
      , "new_value"
      , row_number() over(partition by iph.id, iph.created_date::date order by iph.created_date desc) as rn
    from
      {{ source('salesforce', 'implementation_project_history') }} iph
      inner join {{ source('salesforce', 'implementation_project_c') }} ip on
        iph.parent_id = ip.id
        and iph.created_date >= '2019-01-01'
        and ip.created_date >= '2019-01-01'
      left join salesforce.account a on ip.account_c = a.id
    where
      iph.field = 'Status__c'
     -- and ip.id = 'a0B5h000002EjcnEAC'
  )
  , min_max_project as (
    select
      project_id
      , min(agg_date)
    from
      project_status_history
    group by
      1
  )
 , project_over_time_w_status as (
    select
      * 
      , last_value(country ignore nulls) over(partition by project_id order by date rows unbounded preceding) as country_c
      , last_value(service_offering ignore nulls) over(partition by project_id order by date rows unbounded preceding) as service_offering_c
      , last_value(value ignore nulls) over(partition by project_id order by date rows unbounded preceding) as status
    from
      (
        select
          mmp.project_id
          , d.*
          , psh.new_value as value
          , psh.country
          , psh.service_offering
        from
          dates d
          join min_max_project mmp on
            d.date >= mmp.min
            and d.date <= current_date
          left join project_status_history psh on
            d.date = agg_date
            and mmp.project_id = psh.project_id
            and rn = 1
        order by
          1 asc
      )
  )
select
  cast(date as date) date
  , (
    case
      when country_c is null
        then 'untracked'
      else country_c
    end
  )
  as country
  , (
    case
      when service_offering_c is null
        then 'untracked'
      else service_offering_c
    end
  )
  as service_offering
  , count(
    case
      when status in ('On-Hold')
        then project_id
      else null
    end
  )
  as on_hold_projects
   , count(
    case
      when status in ('Off track', 'Delayed', 'At risk')
        then project_id
      else null
    end
  )
  as red_flag_projects

   , count(
    case
      when status in ('Active', 'New', 'On track')
        then project_id
      else null
    end
  )
  as in_progress_projects

   , count(
    case
      when status in ('Closed','Live', 'Delivered', 'Completed', 'CS')
        then project_id
      else null
    end
  )
  as completed_projects

   , count(
    case
      when status in ('Churned', 'Expired')
        then project_id
      else null
    end
  )
  as churned_projects

  , count(
    case
      when status is null
        then project_id
      else null
    end
  )
  as null_projects
from
  project_over_time_w_status
where
  date >= cast('2019-01-01' as date)
group by
  1
  , 2
  , 3
order by
  date desc