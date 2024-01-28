{% macro limit_in_dev(timestamp) %}
    -- this filter will only be applied in dev run
    {% if target.name != 'prod' %}
        where {{ timestamp }} > dateadd('day', -{{ var('dev_days_of_data', 3) }}, current_date)
    {% else %}
        where 1=1
    {% endif %}
{% endmacro %}
