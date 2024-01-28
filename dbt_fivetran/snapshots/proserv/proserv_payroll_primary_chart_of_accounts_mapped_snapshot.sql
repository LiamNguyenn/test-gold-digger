{% snapshot proserv_payroll_primary_chart_of_accounts_mapped_snapshot %}

{{
    config(
    alias='payroll_primary_chart_of_accounts_mapped_snapshot',
    target_schema="proserv",
    strategy='check',
    unique_key='business_id',
    check_cols=['are_default_primary_accounts_mapped'],
    invalidate_hard_deletes=True,
    )
}}

with journal_default_account as (
  select 
    a.business_id
    , a.account_type
    , bt.country
  from 
    {{ref('keypay_journal_default_account')}} a
    left join {{ref('keypay_business_traits')}} bt on bt.id = a.business_id 
)

-- description for account_type can be found in seeds/keypay/journal_default_account_type

, primary_chart_completed_au as (
  select 
    business_id
    , true as are_default_primary_accounts_mapped
  from (
    select 
      business_id
      , count(distinct account_type) as default_account_count
    from 
      journal_default_account
    where 
      country = 'AU'
      and account_type in (1,2,3,5,6,7,10)
    group by 1
    having default_account_count = 7
  )
)

, primary_chart_completed_uk as (
  select 
    business_id
    , true as are_default_primary_accounts_mapped
  from (
    select 
      business_id
      , count(distinct account_type) as default_account_count
    from 
      journal_default_account
    where 
      country = 'GB'
      and account_type in (1,2,6,7,10,11,12,13,14,15,24,26,27)
    group by 1
    having default_account_count = 13
  )
)

, primary_chart_completed_nz as (
  select 
    business_id
    , true as are_default_primary_accounts_mapped
  from (
    select 
      business_id
      , count(distinct account_type) as default_account_count
    from 
      journal_default_account
    where 
      country = 'NZ'
      and account_type in (1,2,6,7,10,16,17,18,26)
    group by 1
    having  default_account_count = 9
  )
)

, primary_chart_completed_sg as (
  select 
    business_id
    , true as are_default_primary_accounts_mapped
  from (
    select 
      business_id
      , count(distinct account_type) as default_account_count
    from 
      journal_default_account
    where 
      country = 'SG'
      and account_type in (1,2,6,7,10,19,20,21,22,23)
    group by 1
    having default_account_count = 10
  )
)

, primary_chart_completed_my as (
  select 
    business_id
    , true as are_default_primary_accounts_mapped
  from (
    select 
      business_id
      , count(distinct account_type) as default_account_count
    from 
      journal_default_account
    where 
      country = 'MY'
      and account_type in (1,2,6,10,28,29,30,31,32,33,34,35,36,37,38,39,40,41,42,43,44)
    group by 1
    having default_account_count = 21
  )
)

select * from primary_chart_completed_au
union
select * from primary_chart_completed_uk
union
select * from primary_chart_completed_nz
union
select * from primary_chart_completed_sg
union
select * from primary_chart_completed_my

{% endsnapshot %}