{% macro job_title_without_seniority(column_name) %} 
case
    when regexp_replace({{column_name}}, '^(Deputy |Casual )', '', 1, 'i') ~* 'assistant accountant' 
        then INITCAP(trim(regexp_replace(regexp_replace({{column_name}}, '^(Deputy |Casual )', '', 1, 'i'), '^(Graduate |Trainee |Associate |Apprentice |Junior |Intermediate |Senior |Lead |Principal |Chief |Head |Executive |Vice |Managing )', '', 1, 'i'))) 
    when regexp_replace({{column_name}}, '^(Deputy |Casual )', '', 1, 'i') !~* '^(Graduate |Associate |Assistant |Apprentice |Junior |Intermediate |Senior |Lead |Principal |Chief |Head |Executive |Vice |Managing )(of |to |\or |\and )'
        and regexp_replace({{column_name}}, '^(Deputy |Casual )', '', 1, 'i') !~* '^(Chief |Executive |Lead ).*(officer|assistant|generator).*'
    then INITCAP(trim(regexp_replace(regexp_replace({{column_name}}, '^(Deputy |Casual )', '', 1, 'i'), '^(Graduate |Trainee |Associate |Assistant |Apprentice |Junior |Intermediate |Senior |Lead |Principal |Chief |Head |Executive |Vice |Managing )', '', 1, 'i'))) 
    else INITCAP(regexp_replace({{column_name}}, '^(Deputy |Casual )', '', 1, 'i')) end
{% endmacro %}