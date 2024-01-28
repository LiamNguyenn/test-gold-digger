{% test is_numeric(model, column_name) %}

{{ config(severity = 'warn') }}

with validation as (

    select
        {{ column_name }} as numeric_field

    from {{ model }}

),

validation_errors as (

    select
        numeric_field

    from validation
    -- if this is true, then even_field is actually odd!
    where numeric_field != ''
        and numeric_field ~ '^[0-9]+$' = False

)

select *
from validation_errors

{% endtest %}