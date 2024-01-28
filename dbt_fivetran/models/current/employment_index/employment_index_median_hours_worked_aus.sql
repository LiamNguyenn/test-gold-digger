{{ config(alias="median_hours_worked_aus") }}

with
    combined_platform_aus as (
        select * from {{ ref("employment_index_eh_kp_combined_hours") }}
    )
select distinct
    month, median(monthly_hours) over (partition by month) as median_hours_worked
from combined_platform_aus
order by month
