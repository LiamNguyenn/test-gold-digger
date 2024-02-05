

select * from "dev"."stg__keypay"."pay_category" where date_trunc('day', _transaction_date) = '2023-12-01'