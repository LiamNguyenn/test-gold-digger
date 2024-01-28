{% macro get_latest_transaction_date_v2(source_table) %}
    {%- call statement('latest_transaction_date_query', True) -%}
        SELECT TO_CHAR(date_trunc('day', MAX(_transaction_date)),'yyyy-mm-dd') AS latest_transaction_date FROM {{ source_table }}
    {%- endcall -%}
    {%- set results = load_result('latest_transaction_date_query')-%}
    {% if execute %}
        {% set max_timestamp = results.data[0][0] %}
    {% else %}
        {% set max_timestamp = None %}
    {% endif %}
    {%do return(max_timestamp) %}
{% endmacro %}