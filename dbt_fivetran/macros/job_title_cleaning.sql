{% macro job_title_cleaning(column_name) %} 
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
                                    trim(REGEXP_REPLACE({{ column_name }}, '\\([^)]*\\)'))
                                ), '(^|\\W)(act|nsw|nt|qld|sa|tas|vic|wa|new south wales|victoria|queensland|western australia|south australia|tasmania|australian capital territory|northern territory|brisbane|canberra|darwin|hobart|melbourne|perth|sydney)(\\W|$)', ' '), '(^|\\W)(act|nsw|nt|qld|sa|tas|vic|wa|new south wales|victoria|queensland|western australia|south australia|tasmania|australian capital territory|northern territory|brisbane|canberra|darwin|hobart|melbourne|perth|sydney)(\\W|$)', ' ')), '-$'))
                            )))))))))))))
                        , ' - ', ' of '), ' : ', ' of '), ':', ' of '), ' | ', ' of '), '|', ' of '), ', ', ' of '), ',', ' of '))
                    , ' / ', ' and '), '/', ' and '))
                , ' + ', ' and '), '+', 'and'))
            , ' & ', ' and '), '&', ' and '))
        ))
    )), ' the ', ' '), '^[-/]', ''), '[-/]$', '')), '  ', ' '))
, '( of| to| \or| \and)$', '')), '( of| to| \or| \and)$', '')), '( of| to| \or| \and)$', '')), '( of| to| \or| \and)$', ''))
{% endmacro %}