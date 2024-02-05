

select
fnv_hash(_line, fnv_hash(_modified, fnv_hash(_file))) as ID,
column_0 as date,
column_1 as project,
column_2 as elapsed_time,
column_3 as numberofmodels,
column_4 as numberofcommands,
column_5 as numberofservices,
column_6 as numberofjobhandles,
column_7 as numberoftypescripts
from "dev"."keypay_stats"."analysis_project_summary_headerless"