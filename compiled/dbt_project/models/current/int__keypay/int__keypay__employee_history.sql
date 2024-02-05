

select * from "dev"."stg__keypay"."employee_history" where date_trunc('day', _transaction_date) = '2024-02-01'