{{ config(materialized='view') }}

with
email_unsubscribed as (
  select email
  from (
    select
      email,
      unsubscribed,
      row_number() over (partition by email order by updated_at desc) as rn
    from
      {{ source( 'marketo', 'lead') }}
  )
  where
    rn = 1
    and unsubscribed = true
)

select
  u.uuid as external_id,
  u.email
from {{ source('postgres_public', 'users') }} as u
left join email_unsubscribed as eu
  on u.email = eu.email
where
  not u._fivetran_deleted
  and eu.email is not null
