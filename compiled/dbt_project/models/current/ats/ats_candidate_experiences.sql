

with
candidate_experience as (
    select
        id,
        user_id,
        industry_standard_job_title,
        trim(job_title)                                                                                                                                                                                                                                                                                        as trim_job_title
        ,  
case
    when regexp_replace(trim_job_title, '^(Deputy |Casual )', '', 1, 'i') ~* 'assistant accountant' 
        then INITCAP(trim(regexp_replace(regexp_replace(trim_job_title, '^(Deputy |Casual )', '', 1, 'i'), '^(Graduate |Trainee |Associate |Apprentice |Junior |Intermediate |Senior |Lead |Principal |Chief |Head |Executive |Vice |Managing )', '', 1, 'i'))) 
    when regexp_replace(trim_job_title, '^(Deputy |Casual )', '', 1, 'i') !~* '^(Graduate |Associate |Assistant |Apprentice |Junior |Intermediate |Senior |Lead |Principal |Chief |Head |Executive |Vice |Managing )(of |to |\or |\and )'
        and regexp_replace(trim_job_title, '^(Deputy |Casual )', '', 1, 'i') !~* '^(Chief |Executive |Lead ).*(officer|assistant|generator).*'
    then INITCAP(trim(regexp_replace(regexp_replace(trim_job_title, '^(Deputy |Casual )', '', 1, 'i'), '^(Graduate |Trainee |Associate |Assistant |Apprentice |Junior |Intermediate |Senior |Lead |Principal |Chief |Head |Executive |Vice |Managing )', '', 1, 'i'))) 
    else INITCAP(regexp_replace(trim_job_title, '^(Deputy |Casual )', '', 1, 'i')) end
 as job_title_without_seniority,
        lower( 
case when regexp_replace(trim_job_title, '^(Deputy |Casual )', '', 1, 'i') ~ '^(Apprentice |Graduate |Trainee |Junior |Intermediate |Senior |Managing |Lead |Leader |Head |Vice |Manager |Director |Chief )' 
        then trim(regexp_substr(regexp_replace(trim_job_title, '^(Deputy |Casual )', '', 1, 'i'), '^(Apprentice |Graduate |Trainee |Junior |Intermediate |Senior |Managing |Lead |Leader |Head |Vice |Manager |Director |Chief )', 1, 1, 'i'))        
    when regexp_replace(trim_job_title, '^(Deputy |Casual )', '', 1, 'i') !~* '^(Associate |Assistant |Principal |Executive )(of |to )'
        and trim(regexp_substr(regexp_replace(trim_job_title, '^(Deputy |Casual )', '', 1, 'i'), '^(Associate |Assistant |Principal |Executive )', 1, 1, 'i')) != ''
        then trim(regexp_substr(regexp_replace(trim_job_title, '^(Deputy |Casual )', '', 1, 'i'), '^(Associate |Assistant |Principal |Executive )', 1, 1, 'i'))
    when trim_job_title ~ '(^|\\W)Apprentice(\\W|$)' then 'Apprentice'
    when trim_job_title ~ '(^|\\W)Graduate(\\W|$)' then 'Graduate'
    when trim_job_title ~ '(^|\\W)Junior(\\W|$)' then 'Junior'
    when trim_job_title ~ '(^|\\W)Intermediate(\\W|$)' then 'Intermediate'
    when trim_job_title ~ '(^|\\W)Senior(\\W|$)' then 'Senior'    
    when trim_job_title ~ '(^|\\W)Managing(\\W|$)' then 'Managing'
    when trim_job_title ~ '(^|\\W)(Lead|Leader)(\\W|$)' then 'Lead'
    when trim_job_title ~ '(^|\\W)Trainee(\\W|$)' then 'Trainee'
    when trim_job_title ~ '(^|\\W)Head(\\W|$)' then 'Head'
    when trim_job_title ~ '(^|\\W)Vice(\\W|$)' then 'Vice'
    when trim_job_title ~ '(^|\\W)Manager(\\W|$)' then 'Manager'
    when trim_job_title ~ '(^|\\W)Director(\\W|$)' then 'Director'
    when trim_job_title ~ '(^|\\W)Chief(\\W|$)' then 'Chief'
    else null end
)                                                                                                                                                                                                                                                                                                              as job_title_seniority,
        company,
        summary,
        current_job,
        case when coalesce(start_year, end_year) < 1900 or coalesce(start_year, end_year) > 2100 then NULL else to_date(coalesce(start_year, end_year) || '-' || coalesce(case when start_month > 12 or start_month < 1 then NULL else start_month end, 6) || '-' || coalesce(start_day, 1), 'YYYY-MM-DD') end as start_date,
        case when coalesce(end_year, start_year) < 1900 or coalesce(end_year, start_year) > 2100 then NULL else to_date(coalesce(end_year, start_year) || '-' || coalesce(case when end_month > 12 or start_month < 1 then NULL else end_month end, 7) || '-' || coalesce(end_day, 1), 'YYYY-MM-DD') end       as end_date,
        case when current_job then 0 else greatest(0, datediff('month', least(coalesce(end_date, current_date), current_date), current_date)::float / 12) end                                                                                                                                                  as gap_years_to_date,
        greatest(0, datediff('month', least(coalesce(start_date, end_date), current_date), least(coalesce(end_date, current_date), current_date))::float / 12)                                                                                                                                                 as duration_years
    from
        "dev"."postgres_public"."user_employment_histories"
    where not _fivetran_deleted
),

swag_job_profiles as (
    select
        u.uuid         as user_uuid,
        u.id,
        lower(u.email) as email,
        u.created_at   as user_created_at,
        u.updated_at   as user_updated_at,
        ui.created_at,
        ui.updated_at,
        ui.first_name,
        ui.last_name,
        ui.user_verified_at,
        ui.source,
        ui.friendly_id,
        ui.completed_profile,
        ui.public_profile,
        ui.last_public_profile_at,
        ui.phone_number,
        ui.country_code,
        ui.city,
        ui.state_code,
        ui.headline,
        ui.summary,
        ui.marketing_consented_at
    from
        "dev"."postgres_public"."users" as u
    inner join "dev"."postgres_public"."user_infos" as ui
        on
            u.id = ui.user_id
            and not ui._fivetran_deleted
    where
        
    u.email !~* '.*(employmenthero|employmentinnovations|keypay|webscale|thinkei|p2m|power2motivate|test|demo|exacc|sandbox|\\+).*'

        and ui.user_verified_at is not NULL
        and (len(ui.country_code) is NULL or len(ui.country_code) != 3)
)

select
    e.id,
    p.user_uuid,
    p.id                                      as user_id,
    p.email,
    e.trim_job_title                          as job_title,
    e.job_title_without_seniority
    ,  
case 
        when INITCAP(job_title_seniority) in ('Associate', 'Assistant', 'Graduate', 'Apprentice', 'Trainee') then 'Junior'
        when INITCAP(job_title_seniority) = '' or INITCAP(job_title_seniority) is null then 'Intermediate'
        when INITCAP(job_title_seniority) in ('Principal', 'Leader') then 'Lead'
        when INITCAP(job_title_seniority) in ('Managing') then 'Manager'
        when INITCAP(job_title_seniority) in ('Head') then 'Head'
        when INITCAP(job_title_seniority) in ('Vice', 'Executive') then 'Director'
        else INITCAP(job_title_seniority) end 
 as job_title_seniority,
    e.industry_standard_job_title,
    e.company,
    e.summary,
    e.current_job,
    e.start_date,
    e.end_date,
    e.gap_years_to_date,
    e.duration_years
from
    swag_job_profiles as p
inner join candidate_experience as e on p.id = e.user_id