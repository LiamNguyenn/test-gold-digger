
{%- set must_have_columns = ['event_id', 'event_name', 'event_time', 'user_uuid'] -%}

{# 
-- Get list of event sources 
#}
{%- set event_refs = var("exports_braze__events", []) -%}

{#
-- Check that all event sources have the required columns
-- Get event properties columns
#}
{%- set all_event_prop_columns = [] -%}
{%- set all_refs = [] -%}
{%- set event_prop_col_to_aliases = {} -%}
{%- for event in event_refs -%}

    {#
    -- Get event ref by model name
    #}
  {%- set event_ref = event.get("model") -%}
  {%- set rel = ref(event_ref) -%}
  {%- do all_refs.append(rel) -%}

    {#
    -- Check missing default columns
    #}
  {%- set column_names = dbt_utils.get_filtered_columns_in_relation(from=rel) -%}
  {%- set missing_columns = set(must_have_columns) - set(column_names) -%}
  {%- if missing_columns -%}
    {{ exceptions.warn(
        'The following columns are missing from your model: ' + missing_columns | join(', ') ~ ', model_name: ' ~ event_ref
    ) }}
  {%- endif -%}

    {#
    -- Check event prop columns exist
    #}
  {%- set event_prop_column_names = event["event_properties"] -%}
  {%- for event_prop_column_name in event_prop_column_names -%}
    {%- set col_name = event_prop_column_name["name"] -%}
    {%- if col_name not in column_names -%}
        {{ exceptions.warn(
            'The following column are missing from your model: ' ~ col_name 
        ) }}
    {%- endif -%}
    {%- do all_event_prop_columns.append(col_name) -%}
    {%- if "alias" in event_prop_column_name -%}
      {%- do event_prop_col_to_aliases.update({col_name: event_prop_column_name["alias"]}) -%}
    {%- endif -%}
  {%- endfor -%}
{%- endfor -%}


-- Generate union view
{%- if all_refs %}
  with base as (
    {{ dbt_utils.union_relations(
    relations=all_refs,
    include= must_have_columns + all_event_prop_columns
) }}
  ),

  renamed as (
    select
      event_id,
      event_name,
      event_time,
      user_uuid,
      _dbt_source_relation,
      {%- if all_event_prop_columns -%}
        {% for event_prop_col_name in all_event_prop_columns %}
          {%- set alias = event_prop_col_name -%}
          {%- if event_prop_col_name in event_prop_col_to_aliases -%}
            {%- set alias = event_prop_col_to_aliases[event_prop_col_name] -%}
          {%- endif %}
          {{ event_prop_col_name }}                 as event_prop_{{ alias }}{% if not loop.last %},{% endif -%}
        {% endfor %}
      {%- endif %}
    from base
  )

  select * from renamed
{%- endif -%}


