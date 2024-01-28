{%- macro mask_model() -%}
    {# called as a post-hook for an individual model #}
    {%- if execute -%}
        {# Check if user is able to create masking policy #}
        {% set result = run_query(
            "select 1 where user_is_member_of(current_user, 'sys:secadmin')"
        ) %}
        {% if result | length > 0 %}
            {%- set mask_columns = get_columns_to_mask_for_model(
                "model",
                this.database,
                this.schema,
                this.identifier,
                default_policy="no_mask",
            ) %}

            {% if mask_columns %}
                {{ create_masking_policy(mask_columns) }}
                {{ apply_masking_policy(mask_columns) }}
            {% endif %}

        {% endif %}

    {% endif %}

{%- endmacro -%}
