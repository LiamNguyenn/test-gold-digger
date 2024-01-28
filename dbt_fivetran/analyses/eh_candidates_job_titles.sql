with
    employment_history as (
        select distinct c.id, h.job_title as title
        from 
            {{ref('ats_candidate_profiles')}} c
            join {{ source('postgres_public', 'user_employment_histories') }} h on 
                c.id = h.user_id and not h._fivetran_deleted
    )
    , jobs_applied as (
        select distinct c.id, a.job_title as title
        from 
        {{ref('ats_candidate_profiles')}} c
        join {{ref('ats_job_applications')}} a on
            c.email = a.applicant_email
    )
    , candidates_job_titles as (
        select id, REGEXP_REPLACE(title, '(wanted|!|CALL RYAN 0419 625 208)', '', 1, 'i') as title
        from (select * from employment_history union select * from jobs_applied)
        where 
            len(title) > 2 
            and title ~ '.*[a-zA-Z0-9].*' 
            and title not ilike 'i %'
            and title not ilike 'No Experience Required - We Need You'
    )
    , t_cleansed as (
        select title, {{job_title_cleaning('title')}} as t_title, id
        from candidates_job_titles
    )
    , t_common as (
        select t.title, trim(INITCAP(coalesce(m.title_common, t.t_title))) as processed_title, id
        from t_cleansed t 
        left join {{source('csv', 'more_common_job_titles')}} m on t.t_title = m.title_original
    )
    , seniority as (
        select distinct title as original_title
        , {{ job_title_without_seniority('processed_title') }} AS processed_title
        , {{ job_title_seniority('processed_title')}} AS seniority
        , id    
        from t_common
    )

select * from seniority