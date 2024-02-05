select
    *,
    

  to_number(to_char(report_date::DATE,'YYYYMMDD'),'99999999')

 as dim_date_sk
from "dev"."intermediate"."int_enrich_date_spine"