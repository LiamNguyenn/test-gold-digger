




with left_table as (

  select
    user_id as id

  from "dev"."exports"."exports_braze_users"

  where user_id is not null
    and candidate_recent_job_title is not NULL

),

right_table as (

  select
    user_id as id

  from "dev"."postgres_public"."user_employment_histories"

  where user_id is not null
    and job_title is not NULL or industry_standard_job_title is not NULL

),

exceptions as (

  select
    left_table.id,
    right_table.id as right_id

  from left_table

  left join right_table
         on left_table.id = right_table.id

  where right_table.id is null

)

select * from exceptions

