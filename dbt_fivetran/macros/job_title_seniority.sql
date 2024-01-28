{% macro job_title_seniority(column_name) %} 
case when regexp_replace({{column_name}}, '^(Deputy |Casual )', '', 1, 'i') ~ '^(Apprentice |Graduate |Trainee |Junior |Intermediate |Senior |Managing |Lead |Leader |Head |Vice |Manager |Director |Chief )' 
        then trim(regexp_substr(regexp_replace({{column_name}}, '^(Deputy |Casual )', '', 1, 'i'), '^(Apprentice |Graduate |Trainee |Junior |Intermediate |Senior |Managing |Lead |Leader |Head |Vice |Manager |Director |Chief )', 1, 1, 'i'))        
    when regexp_replace({{column_name}}, '^(Deputy |Casual )', '', 1, 'i') !~* '^(Associate |Assistant |Principal |Executive )(of |to )'
        and trim(regexp_substr(regexp_replace({{column_name}}, '^(Deputy |Casual )', '', 1, 'i'), '^(Associate |Assistant |Principal |Executive )', 1, 1, 'i')) != ''
        then trim(regexp_substr(regexp_replace({{column_name}}, '^(Deputy |Casual )', '', 1, 'i'), '^(Associate |Assistant |Principal |Executive )', 1, 1, 'i'))
    when {{column_name}} ~ '(^|\\W)Apprentice(\\W|$)' then 'Apprentice'
    when {{column_name}} ~ '(^|\\W)Graduate(\\W|$)' then 'Graduate'
    when {{column_name}} ~ '(^|\\W)Junior(\\W|$)' then 'Junior'
    when {{column_name}} ~ '(^|\\W)Intermediate(\\W|$)' then 'Intermediate'
    when {{column_name}} ~ '(^|\\W)Senior(\\W|$)' then 'Senior'    
    when {{column_name}} ~ '(^|\\W)Managing(\\W|$)' then 'Managing'
    when {{column_name}} ~ '(^|\\W)(Lead|Leader)(\\W|$)' then 'Lead'
    when {{column_name}} ~ '(^|\\W)Trainee(\\W|$)' then 'Trainee'
    when {{column_name}} ~ '(^|\\W)Head(\\W|$)' then 'Head'
    when {{column_name}} ~ '(^|\\W)Vice(\\W|$)' then 'Vice'
    when {{column_name}} ~ '(^|\\W)Manager(\\W|$)' then 'Manager'
    when {{column_name}} ~ '(^|\\W)Director(\\W|$)' then 'Director'
    when {{column_name}} ~ '(^|\\W)Chief(\\W|$)' then 'Chief'
    else null end
{% endmacro %}