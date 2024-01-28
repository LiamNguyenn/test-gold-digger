{% macro get_existing_masking_policy() %}

    {% set policies = dbt_utils.get_query_results_as_dict(
        "select policy_name from svv_masking_policy"
    ) %}

    {{ return(policies["policy_name"]) }}

{% endmacro %}
