--please consult with Carina Murray for details on the queries below
with
  project_imp_table as (
    select distinct
      ip.project_completion_date_c
      , coalesce(ip.project_completion_date_c, ip.go_live_date_c) as completed_date
      , date(ip.created_date) as created_date
      , (
        case
          when ip.project_completion_date_c is not null
            then datediff('day', ip.created_date, ip.project_completion_date_c)
          when ip.go_live_date_c is not null
            then datediff('day', ip.created_date, ip.go_live_date_c)
          else 0
        end
      )
      as days_to_implement
      , ip.service_offering_c
      , (
        case
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
      , ip.id as proserv_id
      , ip.stage_c
      , ip.status_c
    from
      "dev"."salesforce"."implementation_project_c" ip
      left join "dev"."salesforce"."account" a on
        ip.account_c = a.id
    where
      ip.created_date >= '2019-01-01'
      and ip.project_completion_date_c >= '2019-01-01'
  )
  , churn_table as (
    select
      date(c.created_date) as case_created_date
      , (
        case
          when lower(ac.geo_code_c) = 'au'
            then 'Australia'
          when lower(ac.geo_code_c) = 'uk'
            then 'United Kingdom'
          when lower(ac.geo_code_c) = 'sg'
            then 'Singapore'
          when lower(ac.geo_code_c) = 'my'
            then 'Malaysia'
          when lower(ac.geo_code_c) = 'nz'
            then 'New Zealand'
          else 'untracked'
        end
      )
      as country
      , c.case_number
      , ac.churn_request_date_c
      , date(c.effective_date_c) as case_effective_date
      , c.category_c
    from
      "dev"."salesforce"."case" c
      left join "dev"."salesforce"."account" ac on
        c.account_id = ac.id
    where
      ac.is_deleted = false
      and c.category_c in (
        'Term Amendment'
        , 'Subscription Cancellation'
        , 'Downgrade'
        , 'Full Churn'
      )
  )
--aggregation of metrics
--avg_time_to_implement
select distinct
  completed_date as date
  , country
  , 'service_offering' as sub_type
  , (
    case
      when service_offering_c is null
        then 'untracked'
      else service_offering_c
    end
  )
as sub_value
, 'days_to_implement' as data_type
, sum(completed_date-created_date) as num_value
from
project_imp_table
where
completed_date is not null
group by
1
, 2
, 3
, 4
, 5
union
select distinct
completed_date as date
, country
, 'service_offering' as sub_type
, (
  case
    when service_offering_c is null
      then 'untracked'
    else service_offering_c
  end
)
as sub_value
, 'days_to_implement_projects_number' as data_type
, count(distinct proserv_id) as num_value
from
project_imp_table
where
completed_date is not null
group by
1
, 2
, 3
, 4
, 5
union
--churn
select
case_effective_date as date
-- set it to yesterday as only current record is needed
, country
, 'category' as sub_type
, (
case
  when category_c is null
    then 'untracked'
  else category_c
end
)
as sub_value
, 'churn' as data_type
, count(distinct case_number) as num_value
from
churn_table
where
case_effective_date is not null
group by
1
, 2
, 3
, 4
, 5