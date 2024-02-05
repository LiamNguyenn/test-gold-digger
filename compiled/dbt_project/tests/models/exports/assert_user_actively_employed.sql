with check_actively_employed_true as (
  select user_uuid
  from "dev"."exports"."exports_braze_users"
  -- when user_actively_employed = true user_uuid should exist on employees table
  where
    user_actively_employed = true
    and user_uuid not in (select distinct user_uuid from "dev"."employment_hero"."employees")
),

check_actively_employed_false as (
  select b.user_uuid
  from "dev"."exports"."exports_braze_users" b
  -- when user_actively_employed = false, that user must not have a job
  where
    b.user_actively_employed = false
    and b.user_uuid in (
      select distinct user_uuid
      from "dev"."employment_hero"."employees" e
      where
        1 = 1
        and termination_date is null
        -- User gonna quite their job but already found a new job.
        or (
          select max(termination_date) from "dev"."employment_hero"."employees"
          where user_uuid = e.user_uuid
        ) < (
          select max(start_date) from "dev"."employment_hero"."employees"
          where user_uuid = e.user_uuid
        )
    )
)

select user_uuid from check_actively_employed_true
union all
select user_uuid from check_actively_employed_false