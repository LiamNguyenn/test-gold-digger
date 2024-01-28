{% macro extract_all_json_elements(column_names_to_be_extracted, json_column_name) %}

{% for dict_item in column_names_to_be_extracted %}

    {% for raw_col, formatted_col in dict_item.items() -%}

case when json_extract_path_text({{ json_column_name }}, '{{ raw_col }}')= '' then null else json_extract_path_text({{ json_column_name }}, '{{ raw_col }}') end as {{ formatted_col }}
    
    {% endfor %}

    {%- if not loop.last -%}
    ,
    {%- endif -%}


{% endfor %}

{% endmacro %}