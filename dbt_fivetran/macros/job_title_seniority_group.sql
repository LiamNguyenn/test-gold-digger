{% macro job_title_seniority_group(column_name) %} 
case 
        when INITCAP({{column_name}}) in ('Associate', 'Assistant', 'Graduate', 'Apprentice', 'Trainee') then 'Junior'
        when INITCAP({{column_name}}) = '' or INITCAP({{column_name}}) is null then 'Intermediate'
        when INITCAP({{column_name}}) in ('Principal', 'Leader') then 'Lead'
        when INITCAP({{column_name}}) in ('Managing') then 'Manager'
        when INITCAP({{column_name}}) in ('Head') then 'Head'
        when INITCAP({{column_name}}) in ('Vice', 'Executive') then 'Director'
        else INITCAP({{column_name}}) end 
{% endmacro %}