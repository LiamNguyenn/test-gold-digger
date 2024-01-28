{{ config(materialized="incremental", alias="jobs_events") }}


select
    e.time,
    app_release,
    app_version,
    brand,
    browser,
    current_url,
    device,
    initial_referrer,
    os,
    os_version,
    u.email as user_email,
    user_id
from mp.event e
left join postgres_public.users u on e.user_id = u.uuid
where
    e.time >= '2022-01-01'  -- hard limit on events (lighten query)
    and (current_url like '%jobs.employmenthero%' or current_url like '%jobs.swagapp%')
    and current_url not like '%secure.employmenthero%'  -- from platform
    and (e.name not like '%\\%' or e.name not like '%onload%')  -- filter out test events 
    and (
        user_email
        !~* '.*(employmenthero|keypay|employmentinnovations|thinkei|p2m|power2motivate|test|demo|exacc|sandbox|\\+).*'
        or user_email is null
    )
    -- and e.name in ('Job Application Page - Visit',
    -- 'ATS EH Job Board Filter - Country',
    -- 'ATS EH Job Board Filter - Industry',
    -- 'ATS EH Job Board Filter - Work type',
    -- 'ATS EH Job Board Visits')
    {% if is_incremental() %}

    -- this filter will only be applied on an incremental run        
    and e.time > (select max(time) from {{ this }})

    {% endif %}
