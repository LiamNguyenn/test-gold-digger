

select * from "dev"."stg__keypay"."billing_plan" where date_trunc('day', _transaction_date) = '2024-02-01'