with base as (

  select *
  from "dev"."app_store"."stg__app_store__review"
),

aggregated as (

  select
    date_day,
    app_id,
    app_version,
    sum(rating) as total_rating,
    count(*)    as total_reviews
  from base
  group by 1,2,3
)

select
  date_day,
  app_id,
  app_version,
  round(total_rating * 1.0 / nullif(total_reviews, 0), 4) as avg_rating
from aggregated