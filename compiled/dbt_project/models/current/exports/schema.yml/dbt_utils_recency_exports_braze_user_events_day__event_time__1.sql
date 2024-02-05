






with recency as (

    select 

      
      
        max(event_time) as most_recent

    from "dev"."exports"."exports_braze_user_events"

    

)

select

    
    most_recent,
    cast(

    dateadd(
        day,
        -1,
        getdate()
        )

 as timestamp) as threshold

from recency
where most_recent < cast(

    dateadd(
        day,
        -1,
        getdate()
        )

 as timestamp)

