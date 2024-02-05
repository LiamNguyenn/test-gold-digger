

with
    cohort_w_attributes as (
        select 
            e.*
            ,coalesce(bf.bradford_score, 0) as bradford_factor
            ,case 
                when bradford_factor<=50 then 1
                when bradford_factor>50 and bradford_factor<=100 then 2
                when bradford_factor>100 and bradford_factor<=200 then 3
                when bradford_factor>200 and bradford_factor<=500 then 4
                when bradford_factor>500 then 5
            end as bradford_rating
            ,coalesce(ab.absent_percentage, 0) as absent_rate
            ,case
                when absent_rate<=1.5 then 1
                when absent_rate>1.5 and absent_rate<=3 then 2
                when absent_rate>3 and absent_rate<=4 then 3
                when absent_rate>4 and absent_rate<=5 then 4
                when absent_rate>5 then 5
            end as absence_rating
        from 
            "dev"."employee_scorecard"."employee_scorecard_cohort" as e
            left join "dev"."employee_scorecard"."bradford_factor" as bf on
                e.member_id = bf.member_id
            left join "dev"."employee_scorecard"."absenteeism" as ab on
                e.member_id = ab.member_id
    )
    , median_bradford_factor as (
        select
            organisation_id
            , median(bradford_factor) as bradford_factor_organisation_median
        from
            cohort_w_attributes
        group by 1
    )
    , median_absent_rate as (
        select
            organisation_id
            , median(absent_rate) as absent_rate_organisation_median
        from
            cohort_w_attributes
        group by 1
    )

select
    c.*
    , mbf.bradford_factor_organisation_median
    , mar.absent_rate_organisation_median
from 
    cohort_w_attributes c
    join median_bradford_factor mbf on
        c.organisation_id = mbf.organisation_id
    join median_absent_rate mar on
        c.organisation_id = mar.organisation_id