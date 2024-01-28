{%- macro gen_masking_policy_names(
    database,
    schema,
    table_name,
    column_name,
    data_type,
    character_maximum_length,
    masking_role
) -%}
    {#
    We create two policies for each column. One for the public role and one for the masking role.
    If a user is member of the masking role, the masking policy will be applied. They will see the masked data.
    #}
    {%- set role_masking_policy_name = [
        database,
        schema,
        table_name,
        column_name,
        data_type | replace(" ", "_"),
        masking_role,
    ] | join("_") -%}

    {%- set public_masking_policy_name = [
        database,
        schema,
        table_name,
        column_name,
        data_type | replace(" ", "_"),
        "public",
    ] | join("_") -%}

    {% set tup = (role_masking_policy_name, public_masking_policy_name) %}

    {{ return(tup) }}
{% endmacro %}
