

select * from "dev"."stg__keypay"."pay_day_filing" where date_trunc('day', _transaction_date) = '2024-02-01'