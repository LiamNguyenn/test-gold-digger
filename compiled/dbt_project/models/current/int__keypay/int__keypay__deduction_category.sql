

select * from "dev"."stg__keypay"."deduction_category" where date_trunc('day', _transaction_date) = '2024-02-02'