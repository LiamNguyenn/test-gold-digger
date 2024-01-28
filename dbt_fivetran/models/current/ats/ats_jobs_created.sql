{{ config(alias='jobs_created') }}

with
    demo_organisations as (
    select
        o.*
    from 
        {{ ref('employment_hero_organisations') }} o
        left join {{ source('zuora', 'account') }} za on o.zuora_account_id = za.id
    where
        sub_name ilike '%demo%'
        or za.batch = 'Batch50'
        and not o.is_shadow_data
    )

select
  j.id as job_id
  , o.id as organisation_id  
  , o.country
  , i."title" as industry
  , j.created_at
  , j.updated_at
  , j.title as job_title
  , j.description as job_description
  , case 
      when j.employment_type = 0 then 'full time'
      when j.employment_type = 1 then 'part time'
    end as employment_type
  , js.name as job_sector
  , case
    when status = 0
      then 'open'
    else 'closed'
  end as job_status
  , case 
      when workplace_type = 0
        then true
      when workplace_type = 1
        then true
      when workplace_type = 2
        then false
      when workplace_type = 3
        then false
      else NULL
    end as is_remote_job
  , case
      when j.workplace_type = 0
        then 'Remote'
      when j.workplace_type = 1
        then 'Remote'
      when j.workplace_type = 2
        then 'Hybrid'
      when j.workplace_type = 3
        then 'On-site'
    end as workplace_type
  , case
      when remote_settings.anywhere IS true
        then 'Remote Anywhere'
      when remote_settings.country_code IS NOT null
        then CONCAT('Remote Country: ', remote_settings.country_code)
      when remote_settings.timezone IS NOT NULL
        then CONCAT('Remote Timezone: ', remote_settings.timezone)
    end as candidate_location
  , case
    when demo.id is not null
      then true
    else false
  end as is_test_job
from
  {{ source('ats_public', 'jobs') }} as j
  -- using this just to get the org id that created the job, no need to filter
  join {{ source('postgres_public', 'organisations') }} o on 
    j.organisation_id = o.uuid
    and not o._fivetran_deleted
    and o.id not in (select id from ats.spam_organisations) -- remove SPAM organisations
  left join {{ source('ats_public', 'remote_settings') }} as remote_settings on
    j.id = remote_settings.job_id
    and not remote_settings._fivetran_deleted
  left join demo_organisations as demo on 
    j.organisation_id = demo.uuid
  left join {{ source('postgres_public', 'industry_categories') }} as i on
    o.industry_category_id = i.id 
    and not i._fivetran_deleted
  left join {{ source('ats_public', 'job_sectors') }} as js on
    j.job_sector_id = js.id
where
  not j._fivetran_deleted