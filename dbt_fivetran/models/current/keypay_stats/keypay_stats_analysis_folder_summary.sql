{{ config(
  alias='analysis_folder_summary',
  materialized='view') 
}}

select
fnv_hash(_line, fnv_hash(_modified, fnv_hash(_file))) as ID,
column_0 as date,
column_1 as datatype,
column_2 as project,
column_3 as folder,
column_4 as numberofitems
from {{ source('keypay_stats', 'analysis_folder_summary_headerless') }}