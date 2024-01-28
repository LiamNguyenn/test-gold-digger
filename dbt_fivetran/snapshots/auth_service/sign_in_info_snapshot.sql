{% snapshot sign_in_info_snapshot %}

{{
    config(
    alias='sign_in_info_snapshot',
    target_schema="auth_service",
    strategy='check',
    unique_key='user_id',
    check_cols=['last_sign_in_at'],
    invalidate_hard_deletes=True,
    )
}}

select id, user_id, last_sign_in_at, last_sign_in_ip from {{source('auth_service_public', 'sign_in_info')}}

{% endsnapshot %}