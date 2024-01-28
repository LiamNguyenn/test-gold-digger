{{ config(materialized="table") }}

with raw_records as (
    select e.id as member_id, e.organisation_id, h.title, industry
    from {{ref('employment_hero_employees')}} e 
    join {{ref('employment_hero_organisations')}} as o on e.organisation_id = o.id 
    join 
        (select * from
            {{ source('postgres_public', 'employment_histories') }}
        where
            id in (
            select
                FIRST_VALUE(id) over(partition by member_id order by created_at desc rows between unbounded preceding and unbounded following)
            from
                {{ source('postgres_public', 'employment_histories') }}
            where
                not _fivetran_deleted
            )
        ) as h on e.id = h.member_id
    where e.active
        and o.is_paying_eh
        and h.title is not null and h.title !~ '^$' and len(h.title) !=1
)

, t_cleansed as (
    select title, {{job_title_cleaning('title')}} as t_title,
    member_id, organisation_id, industry
    from raw_records
)

, t_common as (
    select t.title, trim(INITCAP(coalesce(m.title_common, t.t_title))) as processed_title,
    member_id, organisation_id, industry
    from t_cleansed t 
    left join {{source('csv', 'more_common_job_titles')}} m on t.t_title = m.title_original
)

, seniority as (
    select title as original_title
    , {{ job_title_without_seniority('processed_title') }} AS processed_title
    , {{ job_title_seniority('processed_title')}} AS seniority
    , member_id, organisation_id, industry    
    from t_common
)

select * from seniority