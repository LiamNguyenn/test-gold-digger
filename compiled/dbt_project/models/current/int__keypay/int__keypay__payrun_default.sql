

select * from "dev"."stg__keypay"."payrun_default" where date_trunc('day', _transaction_date) = '2024-02-01'