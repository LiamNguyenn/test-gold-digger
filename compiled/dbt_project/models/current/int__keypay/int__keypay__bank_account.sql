

select * from "dev"."stg__keypay"."bank_account" where date_trunc('day', _transaction_date) = '2024-02-01'