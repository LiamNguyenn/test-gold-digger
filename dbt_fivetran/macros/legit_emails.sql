{% macro legit_emails(column_name) %}
    {{ column_name }} !~* '.*(employmenthero|employmentinnovations|keypay|webscale|thinkei|p2m|power2motivate|test|demo|exacc|sandbox|\\+).*'
{% endmacro %}