{% snapshot exports_braze_users_snapshot %}

    {{
        config(
          target_schema=generate_schema_name('snapshots'),
          strategy='check',
          unique_key='user_uuid',
          check_cols='all',
        )
    }}

    select * from {{ ref('exports_braze_users') }}

{% endsnapshot %}
