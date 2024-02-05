

select * from "dev"."stg__keypay"."contribution_info_deduction" where date_trunc('day', _transaction_date) = '2024-02-02'