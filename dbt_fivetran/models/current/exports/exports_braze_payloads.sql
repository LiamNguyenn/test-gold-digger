{{
    config(
        materialized='incremental',
        full_refresh=false,
        on_schema_change='fail',
        sort='updated_at',
        dist='external_id',
    )
}}

{#- Check if the table already exists. -#}
{%- set target_relation = adapter.get_relation(
      database=this.database,
      schema=this.schema,
      identifier=this.name) 
-%}

{%- set table_exists=target_relation is not none -%}

{{ log("Set columns", info=True) }}

{#- Get all columns in the relation. -#}
{% set all_columns = adapter.get_columns_in_relation(ref('exports_braze_users_snapshot')) %}

{# Build a dict of column names to column types #}
{% set column_types = {} %}
{% for col in all_columns %}
    {% do column_types.update({col.name: col.data_type}) %}
{% endfor %}


{#- Mapping columns. -#}
{% set mapping_columns = [
    {"name": "user_uuid", "export_name": "external_id", "datatype": dbt.type_string()},
    {"name": "dbt_updated_at", "export_name": "updated_at", "datatype": dbt.type_timestamp()},
    {"name": "email", "export_name": "email", "datatype": dbt.type_string()},
    {"name": "first_name", "export_name": "first_name", "datatype": dbt.type_string()},
    {"name": "last_name", "export_name": "last_name", "datatype": dbt.type_string()},
    {"name": "phone_number_e164", "export_name": "phone", "datatype": dbt.type_string()},
    {"name": "alpha_two_letter", "export_name": "country", "datatype": dbt.type_string()},
    {"name": "gender", "export_name": "gender", "datatype": dbt.type_string()},
    {"name": "home_city", "export_name": "home_city", "datatype": dbt.type_string()},

    {"name": "user_is_candidate", "export_name": "user_is_candidate", "datatype": dbt.type_string()},
    {"name": "user_date_created", "export_name": "user_date_created", "datatype": dbt.type_string()},
    {"name": "user_actively_employed", "export_name": "user_actively_employed", "datatype": dbt.type_string()},
    {"name": "postcode", "export_name": "postcode", "datatype": dbt.type_string()},
    {"name": "state_code", "export_name": "state", "datatype": dbt.type_string()},
    {"name": "candidate_recent_job_title", "export_name": "candidate_job_title", "datatype": dbt.type_string()},
    {"static_value": "true", "export_name": "swag_app_workspace_user", "datatype": dbt.type_string()},
] %}

{% set exclude_columns = ['external_id','updated_at'] %}
{% set attribute_columns = [] %}
{%- for col in mapping_columns -%}
    {%- if col.export_name|lower not in exclude_columns -%}
        {% do attribute_columns.append(col.export_name|lower) %}
    {%- endif %}
{%- endfor %}

{{ log("Attributes cols" ~ attribute_columns, info=True) }}

with
renamed as (
    select
        {% for col in mapping_columns %}
            {% if col.static_value %}
                cast('{{ col.static_value }}' as {{ col.datatype }})                                                          as {{ col.export_name }}
            {% elif col.datatype == dbt.type_string() %}
                {% set col_type = column_types[col.name] %}
                {% if col_type == 'boolean' %}
                    case when {{ col.name }} then 'true' else 'false' end                  as {{ col.export_name }}
                {% elif 'timestamp' in col_type %}
                    to_char({{ col.name }} at time zone 'UTC', 'yyyy-MM-ddTHH:mm:ss:SSSZ') as {{ col.export_name }}
                {% else %}
                    cast({{ col.name }} as {{ col.datatype }})                                                   as {{ col.export_name }}
                {% endif %}
            {% else %}
                cast({{ col.name }} as {{ col.datatype }})                                             as {{ col.export_name }}
            {% endif %}
            {% if not loop.last %},{% endif %}
        {% endfor %}
    from {{ ref('exports_braze_users_snapshot') }}
    where
        1 = 1
        {% if is_incremental() %}
            -- this filter will only be applied on an incremental run
            and dbt_updated_at at time zone 'UTC' > (select max(updated_at) from {{ this }})
        {% endif %}
        and dbt_valid_to is NULL
),

new_kvpairs as (
    select *
    from (
        select
            external_id,
            updated_at,
            {% for col in attribute_columns %}
                {{ col }}
                {% if not loop.last %},{% endif %}
            {% endfor %}
        from renamed
    ) unpivot (
        value for key in (
            {% for col in attribute_columns %}
                {{ col }}
                {% if not loop.last %},{% endif %}
            {% endfor %}
        )
    )
)

{% if table_exists %}
-- Below pulls in existing records and flattens them
    , loaded_kvpairs as (
        select
            external_id,
            key,
            value,
            updated_at
        from (select
            external_id,
            updated_at,
            json_parse(payload) as payload
        from {{ this }}) as t,
            unpivot t.payload as value at key
        where 1 = 1
        qualify row_number() over (partition by external_id, key order by updated_at desc) = 1
    )
{% endif %}

-- Compares the most recent records currently transmitted to Braze, by key and external_id
-- If the new data is different from the existing data, that data will be transmitted
-- Only the individual cells that have changed will be sent, or external_ids that are completely new
, json_payloads as (
    select
        nkv.external_id,
        '{' || listagg('"' || nkv.key || '":"' || nkv.value || '"', ', ') || '}' as payload_str,
        max(nkv.updated_at)                                                      as updated_at
    from new_kvpairs as nkv
    {% if table_exists %}
        left join loaded_kvpairs as lkv on nkv.external_id = lkv.external_id and nkv.key = lkv.key
        where coalesce(nkv.value, '|') != coalesce(lkv.value, '|')
    {% endif %}
    group by 1
)

select
    external_id,
    payload_str                   as payload,
    updated_at at time zone 'UTC' as updated_at
from json_payloads
where can_json_parse(payload_str)
