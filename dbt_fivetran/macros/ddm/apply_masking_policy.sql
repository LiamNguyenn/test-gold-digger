{%- macro apply_masking_policy(column_policies) -%}

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

            {% set result = dbt_utils.get_query_results_as_dict(
                "select policy_name from svv_attached_masking_policy"
            ) %}
            {%- set attached_masking_policies = result["policy_name"] -%}

            {%- for policy in column_policies -%}

                {% set databse = policy["database"] %}
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

                {%- if role_masking_policy_name in existing_masking_policies and public_masking_policy_name in existing_masking_policies -%}
                    {%- set body = get_mask_body(column_name, data_type) -%}

                    {% do log(
                        modules.datetime.datetime.now().strftime("%H:%M:%S") 
                        ~ "| Info: Apply masking policy: "
                        ~ role_masking_policy_name
                        ~ " to role: "
                        ~ masking_role
                        ~ " and public masking policy: "
                        ~ public_masking_policy_name
                        ~ " to public",
                        info=True,
                    ) %}

                    alter masking policy {{ public_masking_policy_name }} using ({{ body }});
                    alter masking policy {{ role_masking_policy_name }} using ({{ column_name }}); 

                    {% if role_masking_policy_name not in attached_masking_policies %}
                        attach masking policy {{ role_masking_policy_name }}
                        on "{{ database }}".{{ schema }}.{{ table_name }} (
                            {{ column_name }}
                        )
                        to role {{ masking_role }} priority 1
                        ;
                    {% endif %}

                    {% if public_masking_policy_name not in attached_masking_policies %}
                        attach masking policy {{ public_masking_policy_name }}
                        on "{{ database }}".{{ schema }}.{{ table_name }} (
                            {{ column_name }}
                        )
                        to public
                        ;
                    {% endif %}

                {% endif %}

            {%- endfor -%}

        {% endif %}

    {% endif %}

{%- endmacro -%}
