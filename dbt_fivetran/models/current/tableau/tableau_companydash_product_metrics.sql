with
    dates as (
        select distinct dateadd('day', - generated_number::int, current_date) as "date"
        from ({{ dbt_utils.generate_series(upper_bound=731) }})
    ),
    -- swag wallet loaded
    instapay_into_wallets as (
        select
            ht.created_at::date as date,
            (
                case
                    when lower(m.work_country) = 'au'
                    then 'Australia'
                    when lower(m.work_country) = 'gb'
                    then 'United Kingdom'
                    when lower(m.work_country) = 'sg'
                    then 'Singapore'
                    when lower(m.work_country) = 'my'
                    then 'Malaysia'
                    when lower(m.work_country) = 'nz'
                    then 'New Zealand'
                    else 'untracked'
                end
            ) as country,
            sum(amount) as wallet_loaded
        from {{ source("heropay_db_public", "heropay_transactions") }} as ht
        left join {{ source("heropay_db_public", "heropay_balances") }} as hb on ht.heropay_balance_id = hb.id
        left join {{ source("heropay_db_public", "member_infos") }} as mi on mi.heropay_balance_id = hb.id
        left join {{ source("postgres_public", "members") }} m on m.uuid = ht.member_id
        where ht.created_at >= dateadd('day', -750, current_date) and admin_fee = '3'
        group by 1, 2
        order by 1 desc
    ),
    -- time to hire
    time_to_hire as (
        select
            hired_at::date as date,
            (
                case
                    when lower(country) = 'au'
                    then 'Australia'
                    when lower(country) = 'gb'
                    then 'United Kingdom'
                    when lower(country) = 'sg'
                    then 'Singapore'
                    when lower(country) = 'my'
                    then 'Malaysia'
                    when lower(country) = 'nz'
                    then 'New Zealand'
                    else 'untracked'
                end
            ) as country,
            sum(time_to_hire::float) as total_time_to_hire,
            count(time_to_hire::float) as time_to_hire_count,
            avg(time_to_hire::float) as avg_time_to_hire
        from {{ ref("ats_job_applications") }} as a
        where
            portal_name is not null
            and time_to_hire is not null
            and a.is_test_job = false
            and hired_at >= dateadd('day', -750, current_date)
        group by 1, 2
    ),
    -- avg applicants per job
    avg_app_per_job as (
        select
            job_created_at::date as date,
            (
                case
                    when lower(country) = 'au'
                    then 'Australia'
                    when lower(country) = 'gb'
                    then 'United Kingdom'
                    when lower(country) = 'sg'
                    then 'Singapore'
                    when lower(country) = 'my'
                    then 'Malaysia'
                    when lower(country) = 'nz'
                    then 'New Zealand'
                    else 'untracked'
                end
            ) as country,
            count(distinct job_id) total_jobs_with_app,
            count(distinct applicant_email) total_applicants
        from {{ ref("ats_job_applications") }} as a
        where a.is_test_job = false and job_created_at >= dateadd('day', -750, current_date)
        group by 1, 2
        order by 1 desc, 2
    ),
    -- no. candidate applied
    candidate_applied as (
        select
            ja.applied_at::date as date,
            (
                case
                    when lower(cp.country_code) = 'au'
                    then 'Australia'
                    when lower(cp.country_code) = 'gb'
                    then 'United Kingdom'
                    when lower(cp.country_code) = 'sg'
                    then 'Singapore'
                    when lower(cp.country_code) = 'my'
                    then 'Malaysia'
                    when lower(cp.country_code) = 'nz'
                    then 'New Zealand'
                    else 'untracked'
                end
            ) as country,
            count(distinct cp.id) candidate_applied,
            count(distinct case when ja.portal_name = 'Employment Hero Careers' then cp.id end)
            applicants_through_swag_jobs
        from {{ ref("ats_candidate_profiles") }} cp
        inner join {{ ref("ats_job_applications") }} ja on cp.email = ja.applicant_email
        where ja.applied_at >= dateadd('day', -750, current_date)
        group by 1, 2
    ),
    -- no. candidate profiles
    candidate_profiles as (
        select
            user_verified_at::date as date,
            (
                case
                    when lower(cp.country_code) = 'au'
                    then 'Australia'
                    when lower(cp.country_code) = 'gb'
                    then 'United Kingdom'
                    when lower(cp.country_code) = 'sg'
                    then 'Singapore'
                    when lower(cp.country_code) = 'my'
                    then 'Malaysia'
                    when lower(cp.country_code) = 'nz'
                    then 'New Zealand'
                    else 'untracked'
                end
            ) as country,
            count(distinct cp.id) candidate_profiles
        from {{ ref("ats_candidate_profiles") }} cp
        where user_verified_at >= dateadd('day', -750, current_date)
        group by 1, 2
    ),
    -- distinct_jobs_posted
    distinct_job_posted as (
        select
            posted_at::date as date,
            (
                case
                    when lower(country) = 'au'
                    then 'Australia'
                    when lower(country) = 'gb'
                    then 'United Kingdom'
                    when lower(country) = 'sg'
                    then 'Singapore'
                    when lower(country) = 'my'
                    then 'Malaysia'
                    when lower(country) = 'nz'
                    then 'New Zealand'
                    else 'untracked'
                end
            ) as country,
            count(distinct job_id) as jobs_posted
        from {{ ref("ats_jobs_posted") }}
        where is_test_job = false and posted_at >= dateadd('day', -750, current_date)
        group by 1, 2
    ),
    -- accumulated_swag_profile
    acc_swag as (
        select distinct
            cast(d.date as date) as date,
            (
                case
                    when lower(country_code) = 'au'
                    then 'Australia'
                    when lower(country_code) = 'gb'
                    then 'United Kingdom'
                    when lower(country_code) = 'sg'
                    then 'Singapore'
                    when lower(country_code) = 'my'
                    then 'Malaysia'
                    when lower(country_code) = 'nz'
                    then 'New Zealand'
                    else 'untracked'
                end
            ) as country,
            count(distinct case when cp.user_verified_at <= d.date then cp.id else null end) as swag_profile_acc
        from {{ ref("ats_candidate_profiles") }} cp
        cross join dates as d
        group by 1, 2
    )

select
    cast(dates.date as date) as date,
    c.country as country,
    iiw.wallet_loaded,
    tth.total_time_to_hire,
    tth.time_to_hire_count,
    tth.avg_time_to_hire,
    apj.total_jobs_with_app avg_app_total_jobs,
    apj.total_applicants avg_app_total_applicants,
    ca.candidate_applied,
    ca.applicants_through_swag_jobs,
    cp.candidate_profiles,
    jp.jobs_posted,
    swag.swag_profile_acc
from dates
cross join
    (
        select 'Australia' as country
        union
        select 'United Kingdom' as country
        union
        select 'Singapore' as country
        union
        select 'Malaysia' as country
        union
        select 'New Zealand' as country
        union
        select 'untracked' as country
    )
    c
left join instapay_into_wallets iiw on iiw.date = dates.date and c.country = iiw.country
left join time_to_hire tth on tth.date = dates.date and c.country = tth.country
left join avg_app_per_job apj on apj.date = dates.date and c.country = apj.country
left join candidate_applied ca on ca.date = dates.date and c.country = ca.country
left join distinct_job_posted jp on jp.date = dates.date and c.country = jp.country
left join candidate_profiles cp on cp.date = dates.date and c.country = cp.country
left join acc_swag swag on swag.date = dates.date and swag.country = c.country
