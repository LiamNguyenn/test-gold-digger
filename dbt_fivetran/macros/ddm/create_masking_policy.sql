{%- macro create_masking_policy(column_policies) -%}

    {% if column_policies is not iterable %}
        {% do exceptions.raise_compiler_error(
            '"column_policies" must be a list'
        ) %}
    {% endif %}

    {%- if execute -%}

        {% if column_policies %}

            {%- set existing_masking_policies = (
                get_existing_masking_policy()
            ) -%}

            {%- for policy in column_policies -%}

                {% set database = policy["database"] %}
                {% set schema = policy["schema"] %}
                {% set table_name = policy["table"] %}
                {% set column_name = policy["column_name"] %}
                {% set data_type = policy["data_type"] %}
                {% set character_maximum_length = policy[
                    "character_maximum_length"
                ] %}
                {% set masking_role = policy["masking_role"] %}

                {%- set (
                    role_masking_policy_name,
                    public_masking_policy_name,
                ) = gen_masking_policy_names(
                    database,
                    schema,
                    table_name,
                    column_name,
                    data_type,
                    character_maximum_length,
                    masking_role,
                ) -%}

                {% if role_masking_policy_name in existing_masking_policies and public_masking_policy_name in existing_masking_policies %}
                    {% do log(
                        modules.datetime.datetime.now().strftime("%H:%M:%S") 
                        ~ "| Info: Masking policy '"
                        ~ role_masking_policy_name
                        ~ "' already exists for column: "
                        ~ policy["column_name"]
                        ~ ", skipping",
                        info=True,
                    ) %}
                    {% continue %}
                {% endif %}

                {%- set body = get_mask_body(column_name, data_type) -%}

                {% if character_maximum_length %}
                    {% set data_type = "{}({})".format(
                        data_type, character_maximum_length
                    ) %}
                {% endif %}

                {% set create_masking_policy_query %}
                create masking policy {{ role_masking_policy_name }} with ({{ column_name }} {{ data_type }}) 
                using ({{ column_name }});

                create masking policy {{ public_masking_policy_name }} with ({{ column_name}} {{ data_type }})
                using ({{ body }});
                {% endset %}

                {% do run_query(create_masking_policy_query) %}

            {%- endfor -%}

        {% endif %}

    {%- endif -%}

{%- endmacro -%}
