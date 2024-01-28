{% snapshot salary_range_snapshot %}

{{
    config(
    alias='salary_range_snapshot',
    target_schema="salary_guide",
    strategy='check',
    unique_key='id',
    check_cols='all',
    invalidate_hard_deletes=True,
    )
}}

select * from {{ref('salary_range')}}

{% endsnapshot %}