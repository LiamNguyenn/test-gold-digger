

with raw_records as (
    select e.id as member_id, e.organisation_id, h.title, industry
    from "dev"."employment_hero"."employees" e 
    join "dev"."employment_hero"."organisations" as o on e.organisation_id = o.id 
    join 
        (select * from
            "dev"."postgres_public"."employment_histories"
        where
            id in (
            select
                FIRST_VALUE(id) over(partition by member_id order by created_at desc rows between unbounded preceding and unbounded following)
            from
                "dev"."postgres_public"."employment_histories"
            where
                not _fivetran_deleted
            )
        ) as h on e.id = h.member_id
    where e.active
        and o.is_paying_eh
        and h.title is not null and h.title !~ '^$' and len(h.title) !=1
)

, t_cleansed as (
    select title,  
-- remove ending words   
trim(regexp_replace(trim(regexp_replace(trim(regexp_replace(trim(regexp_replace(         
    trim(replace(TRIM(REGEXP_REPLACE(REGEXP_REPLACE(replace(trim(lower(
        -- abbreviations
        trim(job_title_abbreviation_expand( 
            -- replace & with and
            trim(replace(replace(
                -- replace + with and
                trim(replace(replace(
                    -- 5. replace & with and
                    trim(replace(replace(
                        -- 4. replace ! with of
                        trim(replace(replace(replace(replace(replace(replace(replace(
                            -- 3. trim ending special characters
                            trim(trim('&' from trim(trim('/' from trim(trim(':' from trim(trim('|' from trim(trim('-' from trim(trim('|' FROM ( 
                                -- 2. remove state
                                trim(REGEXP_REPLACE(TRIM(REGEXP_REPLACE(REGEXP_REPLACE(lower(   
                                    -- 1. remove content inside bracket
                                    trim(REGEXP_REPLACE(title, '\\([^)]*\\)'))
                                ), '(^|\\W)(act|nsw|nt|qld|sa|tas|vic|wa|new south wales|victoria|queensland|western australia|south australia|tasmania|australian capital territory|northern territory|brisbane|canberra|darwin|hobart|melbourne|perth|sydney)(\\W|$)', ' '), '(^|\\W)(act|nsw|nt|qld|sa|tas|vic|wa|new south wales|victoria|queensland|western australia|south australia|tasmania|australian capital territory|northern territory|brisbane|canberra|darwin|hobart|melbourne|perth|sydney)(\\W|$)', ' ')), '-$'))
                            )))))))))))))
                        , ' - ', ' of '), ' : ', ' of '), ':', ' of '), ' | ', ' of '), '|', ' of '), ', ', ' of '), ',', ' of '))
                    , ' / ', ' and '), '/', ' and '))
                , ' + ', ' and '), '+', 'and'))
            , ' & ', ' and '), '&', ' and '))
        ))
    )), ' the ', ' '), '^[-/]', ''), '[-/]$', '')), '  ', ' '))
, '( of| to| \or| \and)$', '')), '( of| to| \or| \and)$', '')), '( of| to| \or| \and)$', '')), '( of| to| \or| \and)$', ''))
 as t_title,
    member_id, organisation_id, industry
    from raw_records
)

, t_common as (
    select t.title, trim(INITCAP(coalesce(m.title_common, t.t_title))) as processed_title,
    member_id, organisation_id, industry
    from t_cleansed t 
    left join "dev"."csv"."more_common_job_titles" m on t.t_title = m.title_original
)

, seniority as (
    select title as original_title
    ,  
case
    when regexp_replace(processed_title, '^(Deputy |Casual )', '', 1, 'i') ~* 'assistant accountant' 
        then INITCAP(trim(regexp_replace(regexp_replace(processed_title, '^(Deputy |Casual )', '', 1, 'i'), '^(Graduate |Trainee |Associate |Apprentice |Junior |Intermediate |Senior |Lead |Principal |Chief |Head |Executive |Vice |Managing )', '', 1, 'i'))) 
    when regexp_replace(processed_title, '^(Deputy |Casual )', '', 1, 'i') !~* '^(Graduate |Associate |Assistant |Apprentice |Junior |Intermediate |Senior |Lead |Principal |Chief |Head |Executive |Vice |Managing )(of |to |\or |\and )'
        and regexp_replace(processed_title, '^(Deputy |Casual )', '', 1, 'i') !~* '^(Chief |Executive |Lead ).*(officer|assistant|generator).*'
    then INITCAP(trim(regexp_replace(regexp_replace(processed_title, '^(Deputy |Casual )', '', 1, 'i'), '^(Graduate |Trainee |Associate |Assistant |Apprentice |Junior |Intermediate |Senior |Lead |Principal |Chief |Head |Executive |Vice |Managing )', '', 1, 'i'))) 
    else INITCAP(regexp_replace(processed_title, '^(Deputy |Casual )', '', 1, 'i')) end
 AS processed_title
    ,  
case when regexp_replace(processed_title, '^(Deputy |Casual )', '', 1, 'i') ~ '^(Apprentice |Graduate |Trainee |Junior |Intermediate |Senior |Managing |Lead |Leader |Head |Vice |Manager |Director |Chief )' 
        then trim(regexp_substr(regexp_replace(processed_title, '^(Deputy |Casual )', '', 1, 'i'), '^(Apprentice |Graduate |Trainee |Junior |Intermediate |Senior |Managing |Lead |Leader |Head |Vice |Manager |Director |Chief )', 1, 1, 'i'))        
    when regexp_replace(processed_title, '^(Deputy |Casual )', '', 1, 'i') !~* '^(Associate |Assistant |Principal |Executive )(of |to )'
        and trim(regexp_substr(regexp_replace(processed_title, '^(Deputy |Casual )', '', 1, 'i'), '^(Associate |Assistant |Principal |Executive )', 1, 1, 'i')) != ''
        then trim(regexp_substr(regexp_replace(processed_title, '^(Deputy |Casual )', '', 1, 'i'), '^(Associate |Assistant |Principal |Executive )', 1, 1, 'i'))
    when processed_title ~ '(^|\\W)Apprentice(\\W|$)' then 'Apprentice'
    when processed_title ~ '(^|\\W)Graduate(\\W|$)' then 'Graduate'
    when processed_title ~ '(^|\\W)Junior(\\W|$)' then 'Junior'
    when processed_title ~ '(^|\\W)Intermediate(\\W|$)' then 'Intermediate'
    when processed_title ~ '(^|\\W)Senior(\\W|$)' then 'Senior'    
    when processed_title ~ '(^|\\W)Managing(\\W|$)' then 'Managing'
    when processed_title ~ '(^|\\W)(Lead|Leader)(\\W|$)' then 'Lead'
    when processed_title ~ '(^|\\W)Trainee(\\W|$)' then 'Trainee'
    when processed_title ~ '(^|\\W)Head(\\W|$)' then 'Head'
    when processed_title ~ '(^|\\W)Vice(\\W|$)' then 'Vice'
    when processed_title ~ '(^|\\W)Manager(\\W|$)' then 'Manager'
    when processed_title ~ '(^|\\W)Director(\\W|$)' then 'Director'
    when processed_title ~ '(^|\\W)Chief(\\W|$)' then 'Chief'
    else null end
 AS seniority
    , member_id, organisation_id, industry    
    from t_common
)

select * from seniority