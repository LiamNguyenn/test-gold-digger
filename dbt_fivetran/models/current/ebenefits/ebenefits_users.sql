{{ config(alias='users') }}

with
  first_mobile_app_date as (
    select
      member_id
      , min(timestamp) as first_mobile_app_date
    from
      {{ ref('customers_events') }}
    where
      app_version_string is not null
    group by
      1
  )
select
  user_id
  , m.id as member_id
  , m.organisation_id
  , email
  , first_name
  , last_name
  , first_sign_in_at as joined_at
  , first_mobile_app_date
  , personal_mobile_number as mobile
  , eh.title as job_title
  , coalesce(o.name, ee.name) as company
  , case
    when subscription_plan_id in (
      4
      -- Startup Premium
      , 7
      -- Free (30 days)
      , 11
      -- Free
      , 17
      -- Demo
      , 43
      -- CHURN (FREE)
      , 52
      -- Implementations Free
      , 53
      -- Startup Standard
      , 55
      -- ANZ Free
      , 144
      -- International Free
      , 145
      -- Premium Trial
      , 161
      -- SUSPENDED (FREE) 
      , 162 -- SEA free
    )
      then 'free'
    when subscription_plan_id in (
      166 -- ATS Free
    )
      then 'free ats'
    when subscription_plan_id in (
      6
      -- Standard (6)
      , 13
      -- Standard (8)
      , 19
      -- Standard (5)
      , 35
      -- Standard + YY (3)
      , 36
      -- Implementation Standard
      , 38
      -- CSA Standard (5)
      , 39
      -- CSA Standard + YY (3)
      , 48
      -- EOFY CSA Standard (4)
      , 50
      -- Implementations Standard YY
      , 61
      -- YY Standard (1)
      , 63
      -- GB Free Standard
      , 65
      -- AMP Standard (Free)
      , 66
      -- CSA Standard HeroPay 3
      , 68
      -- HeroPay standard 3
      , 72
      -- Reseller Standard
      , 106
      -- Standard (6n)
      , 142
      -- International Standard
      , 149
      -- Standard (6) min 99
      , 160
      -- Zuora Standard
      , 164
      -- UK Organic Standard
      , 165 -- UK CSA Standard
    )
      then 'standard'
    when subscription_plan_id in (
      14
      -- CSA (8)
      , 15
      -- Yin Yang
      , 20
      -- Premium (8)
      , 21
      -- Premium + YY (6)
      , 22
      -- CSA (1.37)
      , 23
      -- CSA (3)
      , 24
      -- CSA (5)
      , 25
      -- CSA (5.5)
      , 26
      -- CSA (6)
      , 27
      -- CSA (6.375)
      , 28
      -- CSA (7)
      , 29
      -- CSA (7.2)
      , 30
      -- CSA (7.5)
      , 37
      -- Implementation Premium
      , 40
      -- CSA Premium (8)
      , 41
      -- CSA Premium + YY (6)
      , 44
      -- EOFY Premium (5)
      , 45
      -- EOFY Premium + YY (3)
      , 46
      -- Premium + YY (0)
      , 47
      -- EOFY Premium + YY (2)
      , 49
      -- CSA Premium (4)
      , 51
      -- Implementations Premium YY
      , 56
      -- ANZ Premium (Free)
      , 58
      -- ANZ Premium (5)
      , 60
      -- YY Premium (4)
      , 64
      -- GB Free Premium
      , 67
      -- CSA Premium HeroPay 5
      , 69
      -- HeroPay Premium 5
      , 70
      -- AON Premium (5)
      , 71
      -- AON Premium (Free)
      , 73
      -- Reseller Premium
      , 107
      -- Premium (9n)
      , 140
      -- AMP Premium (3)
      , 141
      -- AON Premium (4)
      , 143
      -- International Premium
      , 147
      -- CSA Frank (9.11)
      , 150
      -- Premium (9) min 99
      , 152
      -- Premium (9n)
      , 159 -- Zuora Premium
    )
      then 'premium'
    when subscription_plan_id in (
      146
      -- CSA Platinum (14)
      , 148
      -- ANZ Platinum CSA
      , 151
      -- Platinum (14) min 199
      , 153
      -- OE Platinum CSA
      , 154
      -- International Platinum
      , 158 -- Zuora Platinum
    )
      then 'platinum'
    when subscription_plan_id in (
      5
      -- Premium (L)
      , 9
      -- Annual
      , 10
      -- Standard (L)
      , 18
      -- OE
      , 163 -- Legacy
    )
      then 'legacy'
  end as pricing_tier,
  setup_mode,
  benefits.benefits_enabled,
  benefits.instapay_enabled,
  addresses.country
from
  {{ source('postgres_public', 'users') }} u
  join(
    select
      *
    from
      {{ source('postgres_public', 'members') }}
    where
      id in (
        select
          FIRST_VALUE(id) over(partition by user_id order by created_at desc rows between unbounded preceding and unbounded following)
        from
          {{ source('postgres_public', 'members') }}
        where
          not _fivetran_deleted
          and not is_shadow_data
          and not system_manager
          and not system_user
          and not independent_contractor
      )
  )
  as m on
    m.user_id = u.id
  left join {{ source('postgres_public', 'addresses') }} on
  m.address_id = addresses.id
  left join first_mobile_app_date on
    first_mobile_app_date.member_id = m.id
  left join (
    select
      *
    from
      {{ source('postgres_public', 'employment_histories') }}
    where
      id in (
        select
          FIRST_VALUE(id) over(partition by member_id order by created_at desc rows between unbounded preceding and unbounded following)
        from
          {{ source('postgres_public', 'employment_histories') }}
        where
          not _fivetran_deleted
      )
  )
  as eh on
    m.id = eh.member_id
  left join {{ source('postgres_public', 'organisations') }} as o on
  organisation_id = o.id
left join (
  select
    *
  from
    {{ source('postgres_public', 'employing_entities') }}
  where
    id in (
      select
        FIRST_VALUE(id) over(partition by organisation_id order by created_at asc rows between unbounded preceding and unbounded following)
      from
        {{ source('postgres_public', 'employing_entities') }}
    )
)
ee on
  o.id = ee.organisation_id
left join (
  select
    *
  from
    {{ source('postgres_public', 'agreements') }}
  where
    id in (
      select
        FIRST_VALUE(id) over(partition by organisation_id order by created_at desc rows between unbounded preceding and unbounded following)
      from
        {{ source('postgres_public', 'agreements') }}
      where
        not _fivetran_deleted
    )
)
a on
  o.id = a.organisation_id
left join (
  select target_id as uuid, bool_and(name = 'e2p0' and name != 'E2P0 Worklife Blacklisted Orgs') as benefits_enabled, bool_and(name = 'instapay' and name != 'E2P0 Instapay Blacklisted Orgs') as instapay_enabled
  from {{ source('feature_flag_public', 'features') }} as f
  join {{ source('feature_flag_public', 'features_target_objects') }} as fto
  on f.id = fto.feature_id
  join {{ source('feature_flag_public', 'target_objects') }} as tob
  on fto.target_object_id = tob.id
  where name in ('E2P0 Worklife Blacklisted Orgs', 'e2p0', 'Worklife Experiment Blacklist','instapay', 'E2P0 Instapay Blacklisted Orgs')
    and (not fto._fivetran_deleted or fto._fivetran_deleted is null)
    and (not tob._fivetran_deleted or tob._fivetran_deleted is null)
group by 1
) benefits
on benefits.uuid = o.uuid
where
not u.is_shadow_data
and email !~* '.*(employmenthero|employmentinnovations|thinkei|p2m|power2motivate|test|demo|exacc|sandbox|\\+).*'