{{
    config(
        materialized='view'
    )
}}

{% set shop_now_event = 'Click shop now in specific online offer page' %}
{% set cv_upload_event = 'Swag Profile - Populate from CV - Upload CV' %}
{% set cv_complete_event = 'SWAG CV - Complete cv profile' %}
{% set candidate_public_profile = 'SWAG CV - Switched public on' %}

{% set renamed_swag_dotcom_events = [
    {
        'old_name': 'Swag Profile - Populate from CV - Upload CV',
        'new_name': 'candidate_cv_uploaded'
    },
    {
        'old_name': 'SWAG CV - Complete cv profile',
        'new_name': 'candidate_profile_completed'
    },
    {
        'old_name': 'SWAG CV - Switched public on',
        'new_name': 'candidate_public_profile'
    },
    {
        'old_name': 'Click shop now in specific online offer page',
        'new_name': 'user_cashback_clicked_shop_now'
    }
] %}

{% set testing_emails = var('exports_braze__testing_emails', []) %}

{% set in_event_filter = renamed_swag_dotcom_events | map(attribute='old_name') | join("', '") %}

with renamed as (
    select
        event.message_id                               as event_id,
        event.user_id                                  as user_uuid,
        event.timestamp                                as event_time,
        decode(
            event.name,
            {% for event in renamed_swag_dotcom_events %}
                '{{ event.old_name }}', '{{ event.new_name }}'
                {% if not loop.last %},{% endif %}
            {% endfor %}
        )                                              as event_name,

        nullif(event.shopnow_offer_module, ''::text)   as shopnow_offer_module,
        nullif(event.shopnow_offer_type, ''::text)     as shopnow_offer_type,
        nullif(event.shopnow_offer_category, ''::text) as shopnow_offer_category
    from {{ ref('customers_events') }} as event
    

{{ limit_in_dev('event.timestamp') }}
        and event.name in ('{{ in_event_filter }}')
),

{% if testing_emails %}
{% set in_test_emails_filter = testing_emails | join("', '") %}
renamed_test_accounts as (
    select
        message_id       as event_id,
        coalesce(nullif(user_id, ''::text), nullif(user_uuid, ''::text)) as user_uuid,
        timestamp        as event_time,
        decode(name,
            {% for event in renamed_swag_dotcom_events %}
                '{{ event.old_name }}', '{{ event.new_name }}'
                {% if not loop.last %},{% endif %}
            {% endfor %}
        ) as event_name
    from {{ ref('customers_int_events') }}
    {{ limit_in_dev('timestamp') }}
        and name in ('{{ in_event_filter }}')
),

test_emails as (
    select
        r.* 
    from renamed_test_accounts r
    left join {{ source('postgres_public', 'users') }} u
        on r.user_uuid = u.uuid
    where u.email in ('{{ in_test_emails_filter }}')
),
{% endif %}

all_events as (
    select * from renamed
    {% if testing_emails %}
    union all
    select 
        *,
        cast(null as text) as shopnow_offer_module,
        cast(null as text) as shopnow_offer_type,
        cast(null as text) as shopnow_offer_category
    from test_emails
    {% endif %}
),

enriched as (
    select
        e.*,
        c.public_profile as public_profile_enabled
    from all_events as e
    left join {{ ref('ats_candidate_profiles') }} as c
        on
            e.user_uuid = c.user_uuid
            and e.event_name in ('candidate_cv_uploaded', 'candidate_profile_completed', 'candidate_public_profile')
)

select * from enriched