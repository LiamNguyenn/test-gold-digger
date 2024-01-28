with 
    raw_records as (
    select 
        m.id as member_id, m.work_country as country, m.start_date, h.title
    from
        {{ source('postgres_public', 'members') }} as m
        join {{ source('postgres_public', 'users') }} as u on 
            u.id = m.user_id
            and not u.is_shadow_data
            and not u._fivetran_deleted
        join (
            select * 
            from {{ source('postgres_public', 'employment_histories') }}
            where
                id in (
                    select FIRST_VALUE(id) over(partition by member_id order by created_at desc rows between unbounded preceding and unbounded following)
                    from {{ source('postgres_public', 'employment_histories') }}
                    where not _fivetran_deleted
                    )
            ) as h on m.id = h.member_id
    where
        m.organisation_id = 8701 
        and m.active
        and not m.system_manager
        and not m.system_user
        and not m._fivetran_deleted
        and not m.is_shadow_data
        and m.start_date is not null
        --   and start_date<=getdate()
    )
    , t_cleansed as (
        select title, {{job_title_cleaning('title')}} as t_title,
        member_id, country, start_date
        from raw_records
    )
    , t_common as (
        select t.title, trim(INITCAP(coalesce(m.title_common, t.t_title))) as processed_title,
        member_id, country, start_date
        from t_cleansed t 
        left join {{source('csv', 'more_common_job_titles')}} m on t.t_title = m.title_original
    )
    , eh_internal_employees as (
        select title as original_title
        , {{ job_title_without_seniority('processed_title') }} AS processed_title
        , {{ job_title_seniority('processed_title')}} AS seniority
        , member_id, country, start_date
        from t_common
    )

select
    original_title, processed_title
    , case 
        when INITCAP(seniority) in ('Associate', 'Assistant', 'Graduate', 'Apprentice') then 'Junior'
        when INITCAP(seniority) = '' or INITCAP(seniority) is null then 'Intermediate'
        when INITCAP(seniority) in ('Principal') then 'Lead'
        when INITCAP(seniority) in ('Managing') then 'Manager'
        when INITCAP(seniority) in ('Head') then 'Head'
        when INITCAP(seniority) in ('Vice', 'Executive') then 'Director'
        else INITCAP(seniority) end as seniority
    , member_id, country, start_date
from eh_internal_employees