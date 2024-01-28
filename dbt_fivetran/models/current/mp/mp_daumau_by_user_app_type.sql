{{
    config(
        materialized='incremental',
        alias='daumau_by_user_app_type'
    )
}}

with dates as (
select
          DATEADD('day', -generated_number::int, (current_date + 1)) date
        from ({{ dbt_utils.generate_series(upper_bound=365) }})     
        where "date" < (select date_trunc('day', max("timestamp")) from {{ ref('customers_events') }})
{% if is_incremental() %}
        and "date" > (SELECT MAX("date") FROM {{ this }} ) 
{% endif %}   
)
                               
, user_app_events as (
        select
          e.user_id, 
          e.user_email,
  			e.user_type, 
  			case when app_version_string is not null then 'mobile' else 'web' end as app_type, 
  		 e.timestamp
        from
          {{ ref('customers_events') }} as e          
        where          
          e.timestamp < (select date_trunc('day', max("timestamp")) from {{ ref('customers_events') }})
      )
      , user_app_dau as (
        select
          DATE_TRUNC('day', e.timestamp) as "date"
          , e.user_type
          , e.app_type    	
          , count(distinct coalesce(e.user_id, e.user_email)) as daily_users
        from
          user_app_events as e
        where  
        e.user_type != ''
        and e.app_type != ''
        group by
          1, 2,3
        
union
		select
          DATE_TRUNC('day', e.timestamp) as "date"
          , 'any' as user_type
          , e.app_type       	
          , count(distinct coalesce(e.user_id, e.user_email)) as daily_users
        from
          user_app_events as e
        where        
        e.app_type != ''
        group by
          1, 3
        
union
		select
          DATE_TRUNC('day', e.timestamp) as "date"
          , e.user_type
          , 'any' as app_type     	
          , count(distinct coalesce(e.user_id, e.user_email)) as daily_users
        from
          user_app_events as e
         where        
        e.user_type != ''        
        group by
          1, 2
        
union
		select
          DATE_TRUNC('day', e.timestamp) as "date"
          , 'any' as user_type
          , 'any' as app_type	
          , count(distinct coalesce(e.user_id, e.user_email)) as daily_users
        from
          user_app_events as e        
        group by
          1
      )
      , user_app_mau as (
        select
          dates.date
          , e.user_type
          , e.app_type
          , count(distinct coalesce(e.user_id, e.user_email)) as monthly_users
        from
          dates
          join user_app_events as e on
          e.timestamp < dateadd(day, 1, dates.date)
          and e.timestamp > dateadd(day, -29, dates.date)                
        where        
        e.user_type != ''
        and e.app_type != ''        
        group by
          1, 2,3
union
       select
          dates.date
          , 'any' as user_type
          , e.app_type   
          , count(distinct coalesce(e.user_id, e.user_email)) as monthly_users
        from
          dates
          join user_app_events as e on
          e.timestamp < dateadd(day, 1, dates.date)
          and e.timestamp > dateadd(day, -29, dates.date)
                where       
        e.app_type != ''      
        group by
          1,3
        
union
       select
          dates.date
          , e.user_type
          , 'any' as app_type  
          , count(distinct coalesce(e.user_id, e.user_email)) as monthly_users
        from
          dates
          join user_app_events as e on
          e.timestamp < dateadd(day, 1, dates.date)
          and e.timestamp > dateadd(day, -29, dates.date)
                where        
        e.user_type != ''        
        group by
          1,2
        
union
       select
          dates.date
          , 'any' as user_type
          , 'any' as app_type     
          , count(distinct coalesce(e.user_id, e.user_email)) as monthly_users
        from
          dates
          join user_app_events as e on
          e.timestamp < dateadd(day, 1, dates.date)
          and e.timestamp > dateadd(day, -29, dates.date)
        group by
          1
      )

select
      m.date
	, m.user_type
	, m.app_type 
      , coalesce(daily_users, 0) as daily_users
      , monthly_users
      , coalesce(daily_users, 0) / monthly_users :: float as dau_mau
    from
      user_app_mau m
      left join user_app_dau d on
        m.date = d.date and m.user_type = d.user_type and m.app_type = d.app_type