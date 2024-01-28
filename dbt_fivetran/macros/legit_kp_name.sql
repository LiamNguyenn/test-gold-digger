{% macro legit_kp_name(column_name) %}
    {{ column_name }} !~* '(^|[ !@#$%^&*(),.?":{}|<>]|\d+)(test|demo)($|[ !@#$%^&*(),.?":{}|<>]|\d+)'
    and {{ column_name }} not ilike ('%zzz%')
{% endmacro %}